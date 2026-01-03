from fastapi import FastAPI
import uvicorn
import socket
from datetime import datetime

app = FastAPI(title="Agentic Framework API", version="1.0.0")

@app.get("/health")
async def health_check():
    """
    Simple health check endpoint
    """
    services = ["kafka:9092", "redis:6379", "mongodb:27017", "postgres:5432"]
    
    status = {}
    for service in services:
        name, port = service.split(":")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('localhost', int(port)))
            status[name] = "healthy" if result == 0 else "unhealthy"
            sock.close()
        except:
            status[name] = "unhealthy"
    
    healthy_services = sum(1 for s in status.values() if s == "healthy")
    total_services = len(status)
    
    return {
        "status": "healthy" if healthy_services == total_services else "degraded",
        "timestamp": datetime.now().isoformat(),
        "services": status,
        "summary": f"{healthy_services}/{total_services} services healthy",
        "framework": "Agentic Framework v1.0.0"
    }

@app.get("/")
async def root():
    return {"message": "Agentic Framework API", "status": "running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)