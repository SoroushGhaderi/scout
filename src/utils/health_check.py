"""
Health check utilities for monitorin in g system components.
Provides health checks for:
- ClickHouse connection
- Storage access
- Disk space
"""
import shutil
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from .logging_utils import get_logger
from .alerting import get_alert_manager, AlertLevel
def check_disk_space(path: str = ".", threshold_gb: float = 1.0) -> Dict[str, Any]:
    """
    Check available disk space for a given path.
    Args:
        path: Path to check disk space for (default: current directory)
        threshold_gb: Warning threshold in GB (default: 1.0 GB)
    Returns:
        Dictionary with disk space information and status
    """
    logger = get_logger()
    try:
        path_obj = Path(path).resolve()
        total, used, free = shutil.disk_usage(path_obj)
        total_gb = total / (1024**3)
        used_gb = used / (1024**3)
        free_gb = free / (1024**3)
        used_percent = (used / total) * 100 if total > 0 else 0
        if free_gb < threshold_gb:
            status = "critical"
        elif free_gb < threshold_gb * 2:
            status = "warning"
        else:
            status = "healthy"
        return {
            "status": status,
            "path": str(path_obj),
            "total_gb": round(total_gb, 2),
            "used_gb": round(used_gb, 2),
            "free_gb": round(free_gb, 2),
            "used_percent": round(used_percent, 2),
            "threshold_gb": threshold_gb,
            "message": f"Free space: {free_gb:.2f} GB ({free_gb*1024:.0f} MB)"
        }
    except Exception as e:
        logger.error(f"Failed to check disk space for {path}: {e}")
        return {
            "status": "error",
            "path": str(path),
            "error": str(e),
            "message": f"Failed to check disk space: {e}"
        }
def check_storage_access(storage_path: str) -> Dict[str, Any]:
    """
    Check if storage directory is accessible and writable.
    Args:
        storage_path: Path to storage directory
    Returns:
        Dictionary with storage access information and status
    """
    logger = get_logger()
    try:
        path_obj = Path(storage_path).resolve()
        exists = path_obj.exists()
        is_dir = path_obj.is_dir() if exists else False
        writable = False
        if exists and is_dir:
            test_file = path_obj / ".health_check_test"
            try:
                test_file.touch()
                test_file.unlink()
                writable = True
            except Exception:
                writable = False
        if not exists:
            status = "error"
            message = f"Storage path does not exist: {storage_path}"
        elif not is_dir:
            status = "error"
            message = f"Storage path is not a directory: {storage_path}"
        elif not writable:
            status = "error"
            message = f"Storage path is not writable: {storage_path}"
        else:
            status = "healthy"
            message = f"Storage is accessible and writable: {storage_path}"
        return {
            "status": status,
            "path": str(path_obj),
            "exists": exists,
            "is_directory": is_dir,
            "writable": writable,
            "message": message
        }
    except Exception as e:
        logger.error(f"Failed to check storage access for {storage_path}: {e}")
        return {
            "status": "error",
            "path": str(storage_path),
            "error": str(e),
            "message": f"Failed to check storage access: {e}"
        }
def check_clickhouse_connection(
    host: str = "localhost",
    port: int = 8123,
    username: str = "default",
    password: str = "",
    database: str = "default",
    timeout: float = 5.0
) -> Dict[str, Any]:
    """
    Check ClickHouse connection health.
    Args:
        host: ClickHouse server host
        port: ClickHouse HTTP port
        username: ClickHouse username
        password: ClickHouse password
        database: Database name
        timeout: Connection timeout in seconds
    Returns:
        Dictionary with ClickHouse connection information and status
    """
    logger = get_logger()
    try:
        import clickhouse_connect
        import socket
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            host_reachable = True
        except (socket.timeout, socket.error, OSError) as e:
            host_reachable = False
            return {
                "status": "error",
                "host": host,
                "port": port,
                "host_reachable": False,
                "error": f"Host unreachable: {e}",
                "message": f"Cannot reach ClickHouse at {host}:{port}"
            }
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                connect_timeout=timeout
            )
            result = client.query("SELECT 1 as health_check")
            query_successful = result.result_rows[0][0] == 1 if result.result_rows else False
            version_result = client.query("SELECT version()")
            version = version_result.result_rows[0][0] if version_result.result_rows else "unknown"
            client.close()
            if query_successful:
                return {
                    "status": "healthy",
                    "host": host,
                    "port": port,
                    "database": database,
                    "host_reachable": True,
                    "connected": True,
                    "version": version,
                    "message": f"ClickHouse is healthy at {host}:{port}/{database} (version: {version})"
                }
            else:
                return {
                    "status": "error",
                    "host": host,
                    "port": port,
                    "database": database,
                    "host_reachable": True,
                    "connected": True,
                    "query_successful": False,
                    "message": f"ClickHouse connection successful but query failed"
                }
        except Exception as e:
            return {
                "status": "error",
                "host": host,
                "port": port,
                "database": database,
                "host_reachable": True,
                "connected": False,
                "error": str(e),
                "message": f"Failed to connect to ClickHouse: {e}"
            }
    except ImportError:
        return {
            "status": "error",
            "host": host,
            "port": port,
            "error": "clickhouse_connect not installed",
            "message": "clickhouse_connect package is not installed"
        }
    except Exception as e:
        logger.error(f"Failed to check ClickHouse connection: {e}")
        return {
            "status": "error",
            "host": host,
            "port": port,
            "error": str(e),
            "message": f"Failed to check ClickHouse connection: {e}"
        }
def health_check(
    clickhouse_host: Optional[str] = None,
    clickhouse_port: int = 8123,
    clickhouse_username: str = "default",
    clickhouse_password: str = "",
    clickhouse_database: str = "default",
    storage_paths: Optional[list[str]] = None,
    disk_path: str = ".",
    disk_threshold_gb: float = 1.0
) -> Dict[str, Any]:
    """
    Perform comprehensive health check of all system components.
    Args:
        clickhouse_host: ClickHouse host (if None, skips ClickHouse check)
        clickhouse_port: ClickHouse port
        clickhouse_username: ClickHouse username
        clickhouse_password: ClickHouse password
        clickhouse_database: ClickHouse database
        storage_paths: List of storage paths to check (default: ["data"])
        disk_path: Path to check disk space for
        disk_threshold_gb: Disk space warning threshold in GB
    Returns:
        Dictionary with overall health status and component checks
    """
    logger = get_logger()
    if storage_paths is None:
        storage_paths = ["data"]
    results = {
        "timestamp": datetime.now().isoformat(),
        "overall_status": "unknown",
        "components": {}
    }
    if clickhouse_host:
        logger.info(f"Checking ClickHouse connection: {clickhouse_host}:{clickhouse_port}")
        clickhouse_result = check_clickhouse_connection(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_username,
            password=clickhouse_password,
            database=clickhouse_database
        )
        results["components"]["clickhouse"] = clickhouse_result
    else:
        results["components"]["clickhouse"] = {
            "status": "skipped",
            "message": "ClickHouse check skipped (no host provided)"
        }
    storage_results = {}
    for storage_path in storage_paths:
        logger.info(f"Checking storage access: {storage_path}")
        storage_results[storage_path] = check_storage_access(storage_path)
    results["components"]["storage"] = storage_results
    logger.info(f"Checking disk space: {disk_path}")
    disk_result = check_disk_space(disk_path, threshold_gb=disk_threshold_gb)
    results["components"]["disk_space"] = disk_result
    statuses = []
    for component_name, component_result in results["components"].items():
        if isinstance(component_result, dict):
            if "status" in component_result:
                statuses.append(component_result["status"])
        elif isinstance(component_result, dict):
            for path, path_result in component_result.items():
                if isinstance(path_result, dict) and "status" in path_result:
                    statuses.append(path_result["status"])
    if "error" in statuses or "critical" in statuses:
        results["overall_status"] = "unhealthy"
    elif "warning" in statuses:
        results["overall_status"] = "degraded"
    elif all(s in ["healthy", "skipped"] for s in statuses):
        results["overall_status"] = "healthy"
    else:
        results["overall_status"] = "unknown"
    alert_manager = get_alert_manager()
    for component_name, component_result in results["components"].items():
        if isinstance(component_result, dict):
            if "status" in component_result:
                status = component_result.get("status")
                if status in ["error", "critical", "warning"]:
                    message = component_result.get("message", "Unknown issue")
                    alert_manager.alert_health_check_failure(
                        component=component_name,
                        status=status,
                        message=message,
                        context=component_result
                    )
        elif isinstance(component_result, dict):
            for path, path_result in component_result.items():
                if isinstance(path_result, dict):
                    status = path_result.get("status")
                    if status in ["error", "critical", "warning"]:
                        message = path_result.get("message", "Unknown issue")
                        alert_manager.alert_health_check_failure(
                            component=f"{component_name}:{path}",
                            status=status,
                            message=message,
                            context=path_result
                        )
    return results
