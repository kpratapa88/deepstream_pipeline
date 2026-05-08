from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_BROKER: str = "localhost:9092"
    CLIPS_DIR: str = "output/clips"
    BACKEND_PORT: int = 8000
    SNAPSHOT_RESOLUTION: str = "640x360"
    PIPELINE_LAUNCHER: str = "subprocess"
    PIPELINE_CONTAINER: str = ""   # Docker container name; empty = run on host

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
