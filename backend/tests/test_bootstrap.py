from fastapi.testclient import TestClient

from ipaklink_analytics.api.main import create_app
from ipaklink_analytics.config import Settings
from ipaklink_analytics.domain.collection import PolicySnapshot
from ipaklink_analytics.ports.policy import PolicyUnavailable


class Policy:
    def __init__(self, *, enabled: bool = True, schema_versions: list[int] | None = None):
        self.enabled = enabled
        self.schema_versions = [1] if schema_versions is None else schema_versions

    async def get_active(self):
        return PolicySnapshot.model_validate(
            {
                "version": 2,
                "schema_versions": self.schema_versions,
                "rules": {
                    "enabled": self.enabled,
                    "anonymous_enabled": True,
                    "platforms": ["android"],
                },
            }
        )


class UnavailablePolicy:
    async def get_active(self):
        raise PolicyUnavailable


PATH = "/ipaklink/v1/analytics/config"
DEVICE = {
    "platform": "android",
    "appVersion": "1.2.0",
    "installId": "3030e190-a030-4bba-9cec-057d5f419f01",
}


def test_liveness_and_collection_contract(tmp_path):
    with TestClient(create_app(Settings(log_dir=tmp_path), Policy())) as client:
        assert client.get("/health/live").json() == {
            "code": 200,
            "message": "ok",
            "data": {"status": "alive"},
        }
        response = client.get(PATH, params=DEVICE)
        assert response.status_code == 200
        assert response.headers["Cache-Control"] == "private, no-store"
        assert response.headers["X-Request-Id"]
        assert response.json()["data"]["enabled"] is True
        assert response.json()["data"]["schemaVersions"] == [1]
    assert any(tmp_path.glob("api-*.log"))


def test_collection_disabled_without_schemas_and_invalid_input(tmp_path):
    with TestClient(create_app(Settings(log_dir=tmp_path), Policy(schema_versions=[]))) as client:
        assert client.get(PATH, params=DEVICE).json()["data"]["enabled"] is False
        invalid = client.get(PATH, params={**DEVICE, "installId": "bad"})
        assert invalid.status_code == 400
        assert invalid.json() == {"code": 400, "message": "analytics.invalidRequest", "data": None}


def test_unavailable_policy_and_unwired_auth_fail_closed(tmp_path):
    with TestClient(create_app(Settings(log_dir=tmp_path), UnavailablePolicy())) as client:
        assert client.get(PATH, params=DEVICE).status_code == 503
    with TestClient(create_app(Settings(log_dir=tmp_path), Policy())) as client:
        response = client.get(PATH, params=DEVICE, headers={"Authorization": "Bearer invalid"})
        assert response.status_code == 503

