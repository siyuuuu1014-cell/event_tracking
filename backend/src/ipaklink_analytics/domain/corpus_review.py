from typing import Literal

from pydantic import Field, model_validator

from ipaklink_analytics.domain.ingest import WireModel


class ReviewDecision(WireModel):
    itemId: str = Field(pattern=r"^[a-fA-F0-9]{24}$")
    status: Literal["approved","rejected"]
    expectedItemRevision: int = Field(ge=1,le=2147483646,strict=True)
    reasonCode: Literal["reviewPassed","reviewFailed","policyMismatch","qualityIssue"]
    ticketId: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}$")

    @model_validator(mode="after")
    def validate_reason(self):
        if (self.status=="approved")!=(self.reasonCode=="reviewPassed"):
            raise ValueError("Review decision and reason do not match")
        return self
