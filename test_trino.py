from prestodb.dbapi import connect

# 配置Trino连接参数
conn = connect(
    host='10.206.32.40',      # Trino协调节点地址
    port=8080,             # Trino服务端口（默认8080）
    user='ta',  # 用户名（根据集群配置填写）
    catalog='mysql',      # 默认目录（如hive、mysql等）
    schema='hermes',      # 默认schema
    # 可选参数：若集群启用HTTPS或认证
    # http_scheme='https',
    # auth=presto.auth.BasicAuthentication("username", "password")
)

try:
    # 创建游标
    cursor = conn.cursor()

    # 执行SQL查询
    query = "SELECT * FROM hermes_operation_task limit 10"
    cursor.execute(query)

    # 获取所有结果
    rows = cursor.fetchall()

    # 打印列名
    column_names = [desc[0] for desc in cursor.description]
    print(column_names)

    # 打印结果
    for row in rows:
        print(row)

except Exception as e:
    print(f"查询出错: {e}")

finally:
    # 确保关闭连接
    cursor.close()
    conn.close()
