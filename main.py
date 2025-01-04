# tcp_server.py
import asyncio
import json
import logging
from typing import Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from uvicorn import Config, Server

app = FastAPI()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tcp_server")


class TCPServer:
    def __init__(self, host="0.0.0.0", port=8888):
        self.host = host
        self.port = port
        self.clients: Dict[str, asyncio.StreamWriter] = {}  # client_id: writer
        self.server = None

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        client_address = writer.get_extra_info("peername")
        logger.info(f"새 클라이언트 연결: {client_address}")

        client_id = None

        try:
            # 초기 식별 정보 수신 (JSON 형식)
            data = await reader.readline()
            if not data:
                logger.warning(
                    f"클라이언트 {client_address}로부터 데이터가 수신되지 않음."
                )
                writer.close()
                await writer.wait_closed()
                return

            initial_data = json.loads(data.decode())
            client_id = initial_data.get("client_id")
            if not client_id:
                logger.warning(
                    f"클라이언트 {client_address}가 client_id를 제공하지 않음. 연결 종료."
                )
                writer.close()
                await writer.wait_closed()
                return

            # 기존에 같은 client_id로 연결된 클라이언트가 있다면 종료
            if client_id in self.clients:
                old_writer = self.clients[client_id]
                old_writer.close()
                await old_writer.wait_closed()
                logger.info(f"기존 클라이언트 {client_id} 연결 종료.")

            self.clients[client_id] = writer
            logger.info(f"클라이언트 {client_id}가 연결되었습니다.")

            # 환영 메시지 전송
            welcome_message = (
                json.dumps({"message": f"환영합니다, {client_id}님!"}) + "\n"
            )
            writer.write(welcome_message.encode())
            await writer.drain()
            logger.info(f"환영 메시지 전송됨: {client_id}")

            while True:
                data = await reader.readline()
                if not data:
                    logger.info(f"클라이언트 {client_id} 연결 종료.")
                    break
                # 클라이언트로부터 추가 데이터가 있을 경우 처리 (필요 시 구현)
                message = data.decode().strip()
                logger.info(f"클라이언트 {client_id}로부터 메시지 수신: {message}")

        except json.JSONDecodeError:
            logger.error(f"클라이언트 {client_address}로부터 유효하지 않은 JSON 수신.")
        except Exception as e:
            logger.error(f"클라이언트 {client_address} 처리 중 에러 발생: {e}")
        finally:
            if client_id and client_id in self.clients:
                del self.clients[client_id]
                logger.info(f"클라이언트 {client_id} 연결 해제.")
            writer.close()
            await writer.wait_closed()

    async def start_tcp_server(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        addr = self.server.sockets[0].getsockname()
        logger.info(f"TCP 서버가 {addr}에서 실행 중입니다.")

        async with self.server:
            await self.server.serve_forever()

    async def send_notification(self, client_id: str, message: str):
        writer = self.clients.get(client_id)
        if writer:
            try:
                data = json.dumps({"message": message}) + "\n"
                writer.write(data.encode())
                await writer.drain()
                logger.info(f"알림 전송됨: {client_id} -> {message}")
            except Exception as e:
                logger.error(f"알림 전송 실패: {client_id} -> {e}")
        else:
            logger.warning(
                f"알림 전송 대상 클라이언트 {client_id}가 연결되어 있지 않습니다."
            )

    async def broadcast_notification(self, message: str, target_client_ids: List[str]):
        tasks = [self.send_notification(cid, message) for cid in target_client_ids]
        await asyncio.gather(*tasks)


# FastAPI와 TCP 서버 인스턴스 생성
tcp_server = TCPServer()


@app.on_event("startup")
async def startup_event():
    # TCP 서버를 백그라운드에서 실행
    asyncio.create_task(tcp_server.start_tcp_server())
    logger.info("TCP 서버가 백그라운드에서 시작되었습니다.")


@app.on_event("shutdown")
async def shutdown_event():
    # 모든 클라이언트 연결 종료
    for client_id, writer in tcp_server.clients.items():
        writer.close()
        await writer.wait_closed()
        logger.info(f"클라이언트 {client_id} 연결 종료.")

    # TCP 서버 종료
    if tcp_server.server:
        tcp_server.server.close()
        await tcp_server.server.wait_closed()
        logger.info("TCP 서버가 종료되었습니다.")


# FastAPI 엔드포인트 정의
@app.post("/webhook")
async def receive_webhook(request: Request):
    """
    Webhook 엔드포인트
    외부 시스템으로부터 데이터를 수신하고, 특정 클라이언트에게 메시지 전송
    """
    try:
        data = await request.json()
        logger.info(f"Webhook 데이터 수신: {data}")

        # Webhook 데이터에서 대상 클라이언트 ID와 메시지 추출
        target_client_ids = data.get("target_client_ids", [])
        notification_message = data.get("message", "기본 알림 메시지")

        if not target_client_ids:
            logger.warning("대상 클라이언트 ID가 제공되지 않음.")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "failure",
                    "reason": "target_client_ids가 제공되지 않음.",
                },
            )

        # 알림 전송
        await tcp_server.broadcast_notification(notification_message, target_client_ids)
        logger.info(f"알림 전송: {notification_message} -> {target_client_ids}")

        return {"status": "success"}

    except json.JSONDecodeError:
        logger.error("Webhook 데이터가 유효한 JSON 형식이 아님.")
        return JSONResponse(
            status_code=400,
            content={"status": "failure", "reason": "유효한 JSON 형식이 아님."},
        )
    except Exception as e:
        logger.error(f"Webhook 처리 중 에러 발생: {e}")
        return JSONResponse(
            status_code=500, content={"status": "failure", "reason": str(e)}
        )


# 메인 실행 함수
async def main():
    # uvicorn 설정
    config = Config(app=app, host="0.0.0.0", port=8000, log_level="info")
    server = Server(config)

    # TCP 서버와 FastAPI 서버를 동시에 실행
    await asyncio.gather(tcp_server.start_tcp_server(), server.serve())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("서버 중지됨.")
