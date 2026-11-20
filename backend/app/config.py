from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Default: local SQLite so the app runs without Docker.
    # docker-compose overrides this with PostgreSQL.
    database_url: str = "sqlite+aiosqlite:///./dfis.db"
    node_count: int = 50
    countries: list[str] = ["Egypt", "Germany", "USA", "Japan", "Brazil"]
    accounts_per_country: int = 20
    initial_balance: float = 10_000.0
    # 1 simulated millisecond costs this many real milliseconds (keeps demos fast)
    time_scale: float = 0.05
    network_seed: int = 7

    class Config:
        env_prefix = "DFIS_"


settings = Settings()
