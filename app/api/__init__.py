import uvicorn


def serve():
    uvicorn.run("app.api.server:app", host="0.0.0.0", port=8000, log_level="info")
