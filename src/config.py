from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class LocalEnvironmentSettings(BaseSettings):
    AZURITE_CONNECTION_STRING: str
    azure_storage_account_name: str = "devstoreaccount1"
    postgres_port: str = "5432"
    postgres_username: str = "postgres"
    POSTGRES_PASSWORD: str
    kafka_bootstrap_server: str = "pkc-7prvp.centralindia.azure.confluent.cloud:9092"
    kafka_user_name: str = "R2Y2MQCJFA2FEXWT"
    KAFKA_CLIENT_SECRET: str

    # NOTE - loading secrets from .env file for local development. Higher environments should use
    # environment variables populated by the deployment pipeline.
    model_config = SettingsConfigDict(env_file=Path(__file__).parent / ".env")


class DevEnvironmentSettings(BaseSettings):
    # NOTE - below fields will be picked up from respective environment's variable added by deployment pipeline
    AZURITE_CONNECTION_STRING: str
    azure_storage_account_name: str
    postgres_port: str
    postgres_username: str
    POSTGRES_PASSWORD: str
    kafka_bootstrap_server: str
    kafka_user_name: str
    KAFKA_CLIENT_SECRET: str


class QAEnvironmentSettings(BaseSettings):
    # NOTE - below fields will be picked up from respective environment's variable added by deployment pipeline
    AZURITE_CONNECTION_STRING: str
    azure_storage_account_name: str
    postgres_port: str
    postgres_username: str
    POSTGRES_PASSWORD: str
    kafka_bootstrap_server: str
    kafka_user_name: str
    KAFKA_CLIENT_SECRET: str


class UATEnvironmentSettings(BaseSettings):
    # NOTE - below fields will be picked up from respective environment's variable added by deployment pipeline
    AZURITE_CONNECTION_STRING: str
    azure_storage_account_name: str
    postgres_port: str
    postgres_username: str
    POSTGRES_PASSWORD: str
    kafka_bootstrap_server: str
    kafka_user_name: str
    KAFKA_CLIENT_SECRET: str


class PRODEnvironmentSettings(BaseSettings):
    # NOTE - below fields will be picked up from respective environment's variable added by deployment pipeline
    AZURITE_CONNECTION_STRING: str
    azure_storage_account_name: str
    postgres_port: str
    postgres_username: str
    POSTGRES_PASSWORD: str
    kafka_bootstrap_server: str
    kafka_user_name: str
    KAFKA_CLIENT_SECRET: str


def get_config(
    environment: str,
) -> (
    LocalEnvironmentSettings
    | DevEnvironmentSettings
    | QAEnvironmentSettings
    | PRODEnvironmentSettings
):
    # setting environment based config
    if environment == "local":
        return LocalEnvironmentSettings()
    elif environment == "dev":
        return DevEnvironmentSettings()
    elif environment == "qa":
        return QAEnvironmentSettings()
    elif environment == "uat":
        return UATEnvironmentSettings()
    elif environment == "prod":
        return PRODEnvironmentSettings()
    else:
        raise ValueError(
            f"Invalid environment: {environment}. "
            "Valid options are: local, dev, qa, uat, prod."
        )


if __name__ == "__main__":
    # Example usage
    config = get_config("local")
    print(config.AZURITE_CONNECTION_STRING)
    print(config.azure_storage_account_name)
    print(config.postgres_username)
    print(config.POSTGRES_PASSWORD)
