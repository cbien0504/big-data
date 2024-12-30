#!/bin/bash

INDEX_NAME="index_test"
ES_HOST="http://localhost:9200"

MAPPING_JSON=$(cat <<EOF
{
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date"
      },
      "value": {
        "type": "long"
      },
      "userName": {
        "type": "keyword"
      }
    }
  }
}

EOF
)

# Kiểm tra và xóa index nếu tồn tại
if curl -X HEAD -I "${ES_HOST}/${INDEX_NAME}" -o /dev/null -w "%{http_code}" -s | grep -q "200"; then
  echo "Xóa index '${INDEX_NAME}'..."
  curl -X DELETE "${ES_HOST}/${INDEX_NAME}" -H 'Content-Type: application/json' -s
  echo "Index '${INDEX_NAME}' đã được xóa."
else
  echo "Index '${INDEX_NAME}' không tồn tại, tiếp tục tạo mới."
fi

echo "Tạo index '${INDEX_NAME}'..."
RESPONSE=$(curl -X PUT "${ES_HOST}/${INDEX_NAME}" -H 'Content-Type: application/json' -d "${MAPPING_JSON}" -s)

if echo "$RESPONSE" | grep -q '"acknowledged":true'; then
  echo "Index '${INDEX_NAME}' đã được tạo thành công."
else
  echo "Lỗi: Không thể tạo index '${INDEX_NAME}'."
  echo "Phản hồi từ Elasticsearch: $RESPONSE"
  exit 1
fi
