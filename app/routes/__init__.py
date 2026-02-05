"""Route helper patterns.

Pattern for protected endpoints:

```python
from fastapi import APIRouter, Depends
from ..auth import get_current_user

router = APIRouter()

@router.get("/protected-endpoint")
async def protected_route(current_user: str = Depends(get_current_user)):
    # current_user contains authenticated email
    # Automatically returns 401 if not authenticated
    return {"user": current_user}
```

Pattern for optional authentication:

```python
from ..auth import get_optional_user

@router.get("/optional-auth-endpoint")
async def optional_route(current_user: str | None = Depends(get_optional_user)):
    if current_user:
        # User is authenticated
        pass
    else:
        # User is not authenticated
        pass
```
"""
