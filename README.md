# EastMoney Watch

一个基于 React + FastAPI 的股票监控应用，用于跟踪东方财富股票数据的波动和概念变化。

## 项目结构

```
eastmoneywatch/
├── frontend/          # React 前端应用
│   ├── src/
│   │   ├── components/    # UI 组件
│   │   ├── lib/          # 工具函数
│   │   └── App.tsx       # 主应用组件
│   └── package.json
├── backend/           # Python 后端 API
│   ├── main.py           # FastAPI 主应用
│   ├── concepts.py       # 概念数据处理
│   ├── fluctuation.py    # 波动分析
│   ├── get_changes_worker_queue.py  # 数据变化队列
│   ├── static/           # 静态数据文件
│   └── pyproject.toml
├── package.json       # 项目统一管理脚本
└── pnpm-workspace.yaml # pnpm 工作区配置
```

## 技术栈

### 前端
- **React 18** - UI 框架
- **TypeScript** - 类型安全
- **Vite** - 构建工具
- **Tailwind CSS** - 样式框架
- **Radix UI** - 无头组件库
- **Lucide React** - 图标库

### 后端
- **FastAPI** - Web 框架
- **Python 3.13** - 运行时
- **AKShare** - 金融数据获取
- **Pandas** - 数据处理
- **Uvicorn** - ASGI 服务器
- **WebSockets** - 实时通信

### 包管理
- **pnpm** - 前端包管理器
- **uv** - Python 包管理器

## 快速开始

### 环境要求

- Node.js 18+
- Python 3.13+
- pnpm
- uv

### 安装依赖

```bash
# 安装所有依赖（前端 + 后端）
pnpm run install:all
```

### 开发模式

```bash
# 同时启动前后端开发服务器
pnpm run dev

# 或分别启动
pnpm run dev:frontend  # http://localhost:5173
pnpm run dev:backend   # http://localhost:8000
```

### 构建生产版本

```bash
# 构建前后端
pnpm run build

# 生产环境启动
pnpm run start
```

## 可用脚本

在项目根目录下：

- `pnpm run dev` - 并行启动前后端开发服务器
- `pnpm run dev:frontend` - 启动前端开发服务器
- `pnpm run dev:backend` - 启动后端开发服务器
- `pnpm run build` - 构建前后端项目
- `pnpm run build:frontend` - 构建前端项目
- `pnpm run build:backend` - 构建后端可执行文件
- `pnpm run start` - 启动生产环境服务
- `pnpm run install:all` - 安装所有依赖

## API 文档

后端启动后，访问以下地址查看 API 文档：

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 功能特性

- 📊 实时股票数据监控
- 📈 股价波动分析
- 🏷️ 概念板块跟踪
- 🔄 WebSocket 实时更新
- 📱 响应式 UI 设计
- 🎨 现代化界面设计

## 开发指南

### 前端开发

```bash
cd frontend
pnpm dev
```

前端使用 Vite 热重载，修改代码后自动刷新页面。

### 后端开发

```bash
cd backend
uv run uvicorn main:app --reload
```

后端使用 uvicorn 热重载，修改 Python 代码后自动重启服务。

### 添加新依赖

```bash
# 前端依赖
cd frontend && pnpm add <package-name>

# 后端依赖
cd backend && uv add <package-name>
```

## 部署

### Docker 部署

```dockerfile
# 多阶段构建
FROM node:18-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN corepack enable pnpm && pnpm install
COPY frontend/ .
RUN pnpm build

FROM python:3.13-slim AS backend
WORKDIR /app
COPY backend/pyproject.toml backend/uv.lock ./
RUN pip install uv && uv sync
COPY backend/ .
COPY --from=frontend-build /app/frontend/dist ./static
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 传统部署

1. 构建前端静态文件：`pnpm run build:frontend`
2. 将前端 dist 目录部署到 Web 服务器
3. 启动后端服务：`pnpm run start:backend`
4. 配置反向代理将 API 请求转发到后端

## 许可证

[MIT License](./LICENSE)