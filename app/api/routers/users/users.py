import bcrypt
from fastapi import APIRouter, HTTPException

from app.database import pool
from app.config import logger
from pydantic import BaseModel
from app.api.routers.users.models import UserCreate, UserLogin, SubscriptionChangeRequest
from app.users_db_interaction import get_user_plan

# Можно добавить prefix="/users", но чтобы не сломать твой фронтенд, 
# пока оставим пути как были, просто сгруппировав их тегами.
router = APIRouter(tags=["Users"])

@router.post("/register")
async def register_user(user: UserCreate):
    logger.info(f"Регистрация пользователя username={user.username}")

    salt = bcrypt.gensalt()

    hashed_password = bcrypt.hashpw(
        user.password.encode('utf-8'),
        salt
    ).decode('utf-8')

    logger.info(f"Пароль захеширован username={user.username}")

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # Проверяем, существует ли пользователь
            await cur.execute(
                "SELECT user_id FROM users WHERE username = %s",
                (user.username,)
            )

            if await cur.fetchone():
                logger.warning(f"Пользователь уже существует username={user.username}")

                raise HTTPException(
                    status_code=400,
                    detail="Пользователь с таким именем уже существует"
                )

            # Создаем пользователя
            await cur.execute(
                "INSERT INTO users (username, password, plan_id) VALUES (%s, %s, %s) RETURNING user_id",
                (user.username, hashed_password, 1)
            )

            row = await cur.fetchone()

            if row:
                new_user_id = row[0]

                logger.info(f"Пользователь создан user_id={new_user_id}")

            else:
                logger.error(f"Ошибка создания пользователя username={user.username}")

                raise HTTPException(
                    status_code=500,
                    detail="Ошибка при создании юзера"
                )

    return {
        "status": "success",
        "user_id": new_user_id,
        "message": "Новый пользователь создан!"
    }

@router.post("/login")
async def login_user(user: UserLogin):
    logger.info(f"Авторизация username={user.username}")

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT user_id, password, role FROM users WHERE username = %s",
                (user.username,)
            )

            row = await cur.fetchone()

    # Если юзера нет или пароль не совпал, отдаем 401 ошибку
    if not row:
        logger.warning(f"Пользователь не найден username={user.username}")

        raise HTTPException(
            status_code=401,
            detail="Неверный логин или пароль"
        )

    user_id, hashed_password, role = row

    logger.info(f"Пользователь найден user_id={user_id}")

    # Проверяем пароль
    if not bcrypt.checkpw(
        user.password.encode('utf-8'),
        hashed_password.encode('utf-8')
    ):
        logger.warning(f"Неверный пароль user_id={user_id}")

        raise HTTPException(
            status_code=401,
            detail="Неверный логин или пароль"
        )

    logger.info(f"Авторизация успешна user_id={user_id}")

    return {
        "status": "success",
        "user_id": user_id,
        "username": user.username,
        "role": role or "user"
    }   

@router.post("/logout")
async def logout_user(user_id: int):
    """
    Эндпоинт для выхода пользователя. 
    В stateless-архитектуре (без серверных сессий) фронтенд просто удаляет данные у себя, 
    но этот эндпоинт полезен для логгирования или если в будущем добавится blacklist токенов.
    """
    logger.info(f"Логаут пользователя user_id={user_id}")
    
    return {
        "status": "success", 
        "message": "Пользователь успешно вышел из системы"
    }

@router.post("/change_subscription")
async def change_subscription(req: SubscriptionChangeRequest):
    logger.info(f"Запрос на изменение подписки user_id={req.user_id} на {req.target_plan}")

    # Приводим к нижнему регистру для надежности (Free -> free)
    target_plan_clean = req.target_plan.lower().strip()

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # 1. Проверяем, существует ли запрашиваемый тариф в таблице plan
            await cur.execute(
                "SELECT plan_id FROM plans WHERE plan_name = %s",
                (target_plan_clean,)
            )
            plan_row = await cur.fetchone()

            if not plan_row:
                logger.warning(f"Отказ: тариф {target_plan_clean} не найден в БД")
                raise HTTPException(
                    status_code=400, 
                    detail=f"Тариф '{target_plan_clean}' не существует. Доступны: free, pro, ultra."
                )
            
            new_plan_id = plan_row[0]

            # 2. Обновляем план пользователя
            # Используем RETURNING user_id, чтобы одним запросом обновить данные и убедиться, что юзер существует
            await cur.execute(
                "UPDATE users SET plan_id = %s WHERE user_id = %s RETURNING user_id",
                (new_plan_id, req.user_id)
            )
            updated_user = await cur.fetchone()

            if not updated_user:
                logger.warning(f"Отказ: пользователь user_id={req.user_id} не найден при смене тарифа")
                raise HTTPException(status_code=404, detail="Пользователь не найден")

    logger.info(f"Успех: тариф изменен user_id={req.user_id} new_plan={target_plan_clean}")

    return {
        "status": "success",
        "message": f"Подписка успешно изменена на {target_plan_clean}",
        "new_plan_id": new_plan_id,
        "plan_name": target_plan_clean
    }

@router.get("/users/{user_id}")
async def get_user_info(user_id: int):
    """Возвращает информацию о пользователе, включая текущий тарифный план (plan_name)"""
    logger.info(f"Получение информации о пользователе user_id={user_id}")
    
    plan_id, plan_name, role = await get_user_plan(user_id)
            
    return {
        "user_id": user_id,
        "plan_id": plan_id,
        "plan_name": plan_name,
        "role": role
    }

from typing import Optional

@router.get("/admin/llm_requests")
async def get_admin_llm_requests(
    limit: int = 10,
    offset: int = 0,
    sort_col: str = "created_at",
    sort_order: str = "DESC",
    filter_col: Optional[str] = None,
    filter_val: Optional[str] = None
):
    from app.users_db_interaction import get_llm_requests_data
    data, total = await get_llm_requests_data(
        limit=limit,
        offset=offset,
        sort_col=sort_col,
        sort_order=sort_order,
        filter_col=filter_col,
        filter_val=filter_val
    )
    return {
        "data": data,
        "total": total
    }


class UserAdminUpdate(BaseModel):
    role: str
    plan_id: int
    is_active: bool


@router.get("/admin/users")
async def get_admin_users(
    limit: int = 10,
    offset: int = 0,
    sort_col: str = "user_id",
    sort_order: str = "ASC",
    filter_col: Optional[str] = None,
    filter_val: Optional[str] = None
):
    from app.users_db_interaction import get_users_data
    data, total = await get_users_data(
        limit=limit,
        offset=offset,
        sort_col=sort_col,
        sort_order=sort_order,
        filter_col=filter_col,
        filter_val=filter_val
    )
    return {
        "data": data,
        "total": total
    }


@router.get("/admin/plans")
async def get_admin_plans():
    from app.users_db_interaction import get_plans_list
    plans = await get_plans_list()
    return plans


@router.put("/admin/users/{user_id}")
async def update_user_by_admin(user_id: int, req: UserAdminUpdate):
    from app.users_db_interaction import update_user_admin
    success = await update_user_admin(
        target_user_id=user_id,
        role=req.role,
        plan_id=req.plan_id,
        is_active=req.is_active
    )
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Не удалось обновить данные пользователя. Проверьте правильность plan_id."
        )
    return {
        "status": "success",
        "message": f"Пользователь user_id={user_id} успешно обновлен"
    }