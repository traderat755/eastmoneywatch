import os


def get_cors_origins():
    """从环境变量获取CORS origins配置"""
    # 默认的端口列表
    default_ports = ["1420", "5173", "61125"]

    # 从环境变量获取CORS_PORTS
    cors_ports_env = os.getenv("CORS_PORTS")
    if cors_ports_env:
        print(f"[CORS] 从环境变量读取CORS ports: {cors_ports_env}")
        # 支持逗号分隔的多个端口
        ports = [port.strip() for port in cors_ports_env.split(",") if port.strip()]
    else:
        print(f"[CORS] 使用默认CORS ports配置")
        ports = default_ports

    # 为每个端口生成对应的origins
    origins = []
    for port in ports:
        origins.extend([
            f"http://localhost:{port}",
            f"http://127.0.0.1:{port}",
            f"http://0.0.0.0:{port}"
        ])

    # 添加特殊的tauri协议
    origins.append("tauri://localhost")

    print(f"[CORS] 生成的origins: {origins}")
    return origins