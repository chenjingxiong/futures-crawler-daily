#!/bin/bash
# 安装每日期货报告的定时任务

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_JOB="30 15 * * 1-5 $SCRIPT_DIR/run_daily.sh"
CRON_TMP="$SCRIPT_DIR/.crontab_tmp"

echo "=========================================="
echo "期货报告定时任务安装"
echo "=========================================="
echo ""
echo "将安装以下定时任务:"
echo "  时间: 工作日(周一到周五) 下午3点30分"
echo "  脚本: $SCRIPT_DIR/run_daily.sh"
echo ""

# 检查是否已存在
current_crontab=$(crontab -l 2>/dev/null || echo "")

if echo "$current_crontab" | grep -q "run_daily.sh"; then
    echo "⚠️  检测到已存在的定时任务"
    echo ""
    echo "当前定时任务:"
    echo "$current_crontab" | grep "run_daily.sh"
    echo ""
    read -p "是否要删除并重新安装? (y/N): " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        echo "取消安装"
        exit 0
    fi

    # 删除旧的定时任务
    current_crontab=$(echo "$current_crontab" | grep -v "run_daily.sh")
fi

# 添加新的定时任务
echo "$current_crontab" > "$CRON_TMP"
echo "$CRON_JOB # 期货每日报告" >> "$CRON_TMP"

# 安装新的crontab
crontab "$CRON_TMP"
rm "$CRON_TMP"

echo ""
echo "✅ 定时任务安装成功!"
echo ""
echo "已安装的定时任务:"
crontab -l | grep "run_daily.sh"
echo ""
echo "=========================================="
echo "定时任务说明:"
echo "  - 运行时间: 工作日 下午3:30"
echo "  - 报告路径: $SCRIPT_DIR/reports/"
echo "  - 日志路径: $SCRIPT_DIR/logs/"
echo ""
echo "管理命令:"
echo "  查看所有定时任务: crontab -l"
echo "  编辑定时任务: crontab -e"
echo "  删除定时任务: crontab -e (然后删除对应行)"
echo ""
echo "手动运行测试:"
echo "  $SCRIPT_DIR/run_daily.sh"
echo "=========================================="
