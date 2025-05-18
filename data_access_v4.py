from fastapi import FastAPI, Request, HTTPException, Depends
from starlette.responses import StreamingResponse, Response
from typing import Optional, Dict, Any, List, AsyncGenerator
import httpx
import boto3
import botocore
import asyncio


from creds import PROVIDERS



app = FastAPI()

CHUNK_SIZE = 1024 * 1024  # 1MB Chunks
# CHUNK_SIZE = (1024 * 1024)*100  # 1MB Chunks
ACCEPTED_DATA_TYPES = (".jp2", ".tif", ".tiff", ".nc", "jpg", ".zip", ".nc:1", ".tif:1")


# Mime-Type Mapping
MIME_TYPES = {
    "tif": "image/tiff",
    "jp2": "image/jp2",
    "jpg": "image/jpeg",
    "png": "image/png",
}


async def stream_s3_object(
        bucket: str,
        key: str,
        provider_id: str,
        range_header: Optional[str] = None,
) -> AsyncGenerator[bytes, None]:
    """
    Streame ein S3-Objekt in Chunks
    Unterstützt Range-Requests
    """
    # s3_client = get_s3_client()
    s3_client = get_s3_client_for_provider(provider_id)

    # Range Header verarbeiten
    start_byte = 0
    end_byte = None

    if range_header:
        range_parts = range_header.replace("bytes=", "").split("-")
        if len(range_parts) >= 1 and range_parts[0]:
            start_byte = int(range_parts[0])
        if len(range_parts) >= 2 and range_parts[1]:
            end_byte = int(range_parts[1])

    # S3 GetObject Parameter
    get_object_kwargs = {
        "Bucket": bucket,
        "Key": key,
        "RequestPayer": "requester"
    }

    if range_header:
        range_value = f"bytes={start_byte}-{end_byte if end_byte is not None else ''}"
        get_object_kwargs["Range"] = range_value

    try:
        # Head-Request für Metadaten
        head_response = s3_client.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
        total_size = head_response['ContentLength']

        # GetObject für Streaming
        response = s3_client.get_object(**get_object_kwargs)
        body = response["Body"]

        # Streaming in Chunks
        chunk = body.read(CHUNK_SIZE)
        while chunk:
            yield chunk
            # Nutzen eines Event-Loops für Asynchronität
            await asyncio.sleep(0)
            chunk = body.read(CHUNK_SIZE)

    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "NoSuchKey":
            raise HTTPException(status_code=404, detail=f"Datei nicht gefunden: {key}")
        elif error_code == "AccessDenied":
            raise HTTPException(status_code=403, detail=f"Zugriff verweigert: {key}")
        else:
            raise HTTPException(status_code=500, detail=f"S3 Fehler: {str(e)}")
    finally:
        if 'body' in locals():
            body.close()



def get_s3_client_for_provider(provider_id: str):
    """
    S3 Client für einen bestimmten Provider erstellen
    """
    if provider_id not in PROVIDERS:
        raise ValueError(f"Unbekannter Provider: {provider_id}")

    provider_config = PROVIDERS[provider_id]

    if provider_config["type"] != "s3":
        raise ValueError(f"Provider {provider_id} unterstützt kein S3")

    session = boto3.Session(
        aws_access_key_id=provider_config["access_key"],
        aws_secret_access_key=provider_config["secret_key"]
    )

    client = session.client(
        's3',
        endpoint_url=f"https://{provider_config['endpoint']}",
        config=botocore.config.Config(signature_version='s3v4'),
        region_name=provider_config['region']
    )

    return client


def get_mime_type(file_path: str) -> str:
    """
    Ermittle den MIME-Type anhand der Dateiendung
    """
    ext = file_path.split(".")[-1].lower()
    return MIME_TYPES.get(ext, "application/octet-stream")


# def get_s3_client_for_provider(provider_id: str):
#     """
#     S3 Client für einen bestimmten Provider erstellen
#     """
#     if provider_id not in PROVIDERS:
#         raise ValueError(f"Unbekannter Provider: {provider_id}")
#
#     provider_config = PROVIDERS[provider_id]
#
#     if provider_config["type"] != "s3":
#         raise ValueError(f"Provider {provider_id} unterstützt kein S3")
#
#     session = boto3.Session(
#         aws_access_key_id=provider_config["access_key"],
#         aws_secret_access_key=provider_config["secret_key"]
#     )
#
#     client = session.client(
#         's3',
#         endpoint_url=f"https://{provider_config['endpoint']}",
#         config=botocore.config.Config(signature_version='s3v4'),
#         region_name=provider_config['region']
#     )
#
#     return client


@app.get("/{provider_id}/{path:path}")
async def proxy_object(provider_id: str, path: str, request: Request):
    """
    Dynamischer Proxy-Endpunkt für Objekte
    Format: /{provider_id}/{pfad/zur/datei}
    """
    if provider_id not in PROVIDERS:
        raise HTTPException(status_code=404, detail=f"Unbekannter Provider: {provider_id}")

    if not path.lower().endswith(ACCEPTED_DATA_TYPES):
        # alle anderen Endungen (XML, AUX, OVR, …) bekommen direkt 404
        raise HTTPException(status_code=404, detail=f"Nicht unterstützte Datei: {path}")

    provider_config = PROVIDERS[provider_id]
    range_header = request.headers.get("Range")

    try:
        if provider_config["type"] == "s3":
            # S3 Provider verarbeiten
            bucket = provider_config["bucket"]
            key = path

            # Metadaten für HEAD-Request
            s3_client = get_s3_client_for_provider(provider_id)
            head_response = s3_client.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
            content_length = head_response['ContentLength']

            # Header vorbereiten
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
            }

            # Range-Request
            status_code = 200
            if range_header:
                status_code = 206
                range_parts = range_header.replace("bytes=", "").split("-")
                start_byte = int(range_parts[0]) if range_parts[0] else 0
                end_byte = int(range_parts[1]) if len(range_parts) > 1 and range_parts[1] else content_length - 1

                headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{content_length}"
                headers["Content-Length"] = str(end_byte - start_byte + 1)

            # MIME-Type bestimmen
            mime_type = get_mime_type(key)

            # Streaming Response
            return StreamingResponse(
                stream_s3_object(bucket, key, provider_id, range_header),
                status_code=status_code,
                media_type=mime_type,
                headers=headers
            )

        elif provider_config["type"] == "http":
            # HTTP Provider verarbeiten
            target_url = f"{provider_config['base_url'].rstrip('/')}/{path}"

            # HTTP-basierter Proxy mit httpx
            req_headers = {header: value for header, value in request.headers.items()
                           if header.lower() not in ['host', 'connection']}

            async def upstream_stream():
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    async with client.stream("GET", target_url, headers=req_headers, timeout=120.0) as resp:
                        try:
                            resp.raise_for_status()
                        except httpx.HTTPStatusError as e:
                            #logger.error(f"HTTP error: {e.response.status_code} - {str(e)}")
                            raise HTTPException(status_code=e.response.status_code, detail=str(e))

                        # Jeden Chunk weiterreichen
                        try:
                            async for chunk in resp.aiter_bytes():
                                yield chunk
                        except (httpx.StreamClosed, asyncio.CancelledError):
                            #logger.warning("Stream closed or cancelled")
                            return

            # Header vorbereiten für HTTP Response
            resp_headers = {"Accept-Ranges": "bytes"}
            status_code = 200

            # Pre-flight request um Header zu bekommen
            async with httpx.AsyncClient(follow_redirects=True) as client:
                # Wenn Range-Header vorhanden, führen wir den Request durch
                if range_header:
                    req_headers["Range"] = range_header
                    head_resp = await client.head(target_url, headers=req_headers, timeout=30.0)

                    # Status und Header übernehmen für Range-Requests
                    status_code = head_resp.status_code  # Sollte 206 sein für Range-Requests

                    # Wichtige Headers übernehmen
                    if "Content-Range" in head_resp.headers:
                        resp_headers["Content-Range"] = head_resp.headers["Content-Range"]
                        #logger.info(f"HTTP range response: status={status_code}, range={resp_headers['Content-Range']}")

                    if "Content-Length" in head_resp.headers:
                        resp_headers["Content-Length"] = head_resp.headers["Content-Length"]
                else:
                    # Für normale Requests HEAD-Request für Metadaten
                    head_resp = await client.head(target_url, headers=req_headers, timeout=30.0)

                    if "Content-Length" in head_resp.headers:
                        resp_headers["Content-Length"] = head_resp.headers["Content-Length"]

            return StreamingResponse(
                upstream_stream(),
                status_code=status_code,
                media_type=get_mime_type(path),
                headers=resp_headers
            )

        else:
            raise HTTPException(status_code=501, detail=f"Provider-Typ nicht unterstützt: {provider_config['type']}")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "NoSuchKey":
            raise HTTPException(status_code=404, detail=f"Datei nicht gefunden: {path}")
        elif error_code == "AccessDenied":
            raise HTTPException(status_code=403, detail=f"Zugriff verweigert: {path}")
        else:
            raise HTTPException(status_code=500, detail=f"S3 Fehler: {str(e)}")


@app.head("/{provider_id}/{path:path}")
async def head_object(provider_id: str, path: str, request: Request):
    """
    HEAD-Request für Objekte
    Format: /{provider_id}/{pfad/zur/datei}
    """
    if provider_id not in PROVIDERS:
        raise HTTPException(status_code=404, detail=f"Unbekannter Provider: {provider_id}")

    if not path.lower().endswith(ACCEPTED_DATA_TYPES):
        # alle anderen Endungen (XML, AUX, OVR, …) bekommen direkt 404
        raise HTTPException(status_code=404, detail=f"Nicht unterstützte Datei: {path}")

    provider_config = PROVIDERS[provider_id]

    try:
        if provider_config["type"] == "s3":
            # S3 Provider verarbeiten
            bucket = provider_config["bucket"]
            key = path

            # Metadaten abrufen
            s3_client = get_s3_client_for_provider(provider_id)
            head_response = s3_client.head_object(Bucket=bucket, Key=key, RequestPayer="requester")
            content_length = head_response['ContentLength']

            # MIME-Type bestimmen
            mime_type = get_mime_type(key)

            # Response erstellen (nur Header, kein Body)
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
            }

            return Response(
                status_code=200,
                media_type=mime_type,
                headers=headers
            )

        elif provider_config["type"] == "http":
            # HTTP Provider verarbeiten
            target_url = f"{provider_config['base_url'].rstrip('/')}/{path}"

            async with httpx.AsyncClient(follow_redirects=True) as client:
                resp = await client.head(target_url, timeout=30.0)
                resp.raise_for_status()

            headers = {
                "Accept-Ranges": resp.headers.get("Accept-Ranges", "bytes"),
            }
            if "Content-Length" in resp.headers:
                headers["Content-Length"] = resp.headers["Content-Length"]
            if "Content-Range" in resp.headers:
                headers["Content-Range"] = resp.headers["Content-Range"]

            return Response(
                status_code=resp.status_code,
                media_type=get_mime_type(path),
                headers=headers
            )

        else:
            raise HTTPException(status_code=501, detail=f"Provider-Typ nicht unterstützt: {provider_config['type']}")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "NoSuchKey":
            raise HTTPException(status_code=404, detail=f"Datei nicht gefunden: {path}")
        elif error_code == "AccessDenied":
            raise HTTPException(status_code=403, detail=f"Zugriff verweigert: {path}")
        else:
            raise HTTPException(status_code=500, detail=f"S3 Fehler: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)

