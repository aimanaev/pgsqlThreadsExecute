from pydantic_settings import BaseSettings

class DatabaseSettings(BaseSettings):
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_HOST: str = "localhost"
    DATABASE_PORT: int = 5432
    DATABASE_DB: str

    DATABASE_CONNECTIONS_MIN: int = 1
    DATABASE_CONNECTIONS_MAX: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra='ignore'

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_DB}"
    
    @property
    def DATABASE_PARAMS(self) -> str:
        return {
            'host': self.DATABASE_HOST,
            'port': self.DATABASE_PORT,
            'database': self.DATABASE_DB,
            'user': self.DATABASE_USER,
            'password': self.DATABASE_PASSWORD
        }
    
settings = DatabaseSettings()