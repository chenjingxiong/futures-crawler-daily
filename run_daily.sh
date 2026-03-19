#!/bin/bash
# 每日期货报告自动生成脚本

# 设置工作目录
cd "$(dirname "$0")"

# 设置环境变量
export PATH=/usr/local/bin:/usr/bin:/bin

# 记录日志
LOG_FILE="./logs/daily_report_$(date +%Y%m%d).log"
mkdir -p logs

echo "========================================" | tee -a "$LOG_FILE"
echo "开始执行每日期货报告任务" | tee -a "$LOG_FILE"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# 激活虚拟环境（如果使用）
# if [ -d "venv" ]; then
#     source venv/bin/activate
# fi

# 运行Python脚本（仅真实数据版本，无模拟数据）
python3 daily_report_real_only.py 2>&1 | tee -a "$LOG_FILE"

# 检查执行结果
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "✅ 任务执行成功" | tee -a "$LOG_FILE"
else
    echo "❌ 任务执行失败" | tee -a "$LOG_FILE"
    exit 1
fi

echo "========================================" | tee -a "$LOG_FILE"
echo "任务完成" | tee -a "$LOG_FILE"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
