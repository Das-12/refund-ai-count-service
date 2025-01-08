from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI
import os

# MONGO_DETAILS = "mongodb+srv://muhammedarshadm:QgZEv11DThwYkC1y@cluster0.mfwgw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"  # Replace with your MongoDB URI
MONGO_DETAILS = os.getenv("MONGO_URI", "mongodb+srv://localhost:27017")
client = AsyncIOMotorClient(MONGO_DETAILS)
database = client.logs
counts_collection = database.get_collection("counts")
