# 需要先安装：
pip install -e .

## note:
macOS/Homebrew Python 可能会遇到 `externally-managed-environment` 报错，这是 PEP 668 对系统 Python 环境的保护；部分 Linux 发行版也可能遇到，Windows 普通 Python 安装通常不会。建议使用虚拟环境安装：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

mac以后每次重新打开终端，都需要先进入项目并激活虚拟环境：
source .venv/bin/activate


# 两种独立启动方式：
1. Web UI，适合直接在web页面手动发起特定投研
tradingagents-web

2. 启动api服务，共其他项目以 RESTful API 方式调用astock投研
tradingagents-research-api

