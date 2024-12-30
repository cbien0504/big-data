#!/bin/bash

if curl -X HEAD -I "http://elasticsearch:9200/index_tweets" -o /dev/null -w "%{http_code}" -s | grep -q "200"; then
  echo "Xóa index 'tweets'..."
  curl -X DELETE "http://elasticsearch:9200/index_tweets"
fi
echo "Tạo index 'tweets'..."
curl -X PUT "http://elasticsearch:9200/index_tweets" -H 'Content-Type: application/json' -d @/es/tweets_mapping.json

if curl -X HEAD -I "http://elasticsearch:9200/index_projects" -o /dev/null -w "%{http_code}" -s | grep -q "200"; then
  echo "Xóa index 'projects'..."
  curl -X DELETE "http://elasticsearch:9200/index_projects"
fi
echo "Tạo index 'projects'..."
curl -X PUT "http://elasticsearch:9200/index_projects" -H 'Content-Type: application/json' -d @/es/projects_mapping.json

if curl -X HEAD -I "http://elasticsearch:9200/index_users" -o /dev/null -w "%{http_code}" -s | grep -q "200"; then
  echo "Xóa index 'users'..."
  curl -X DELETE "http://elasticsearch:9200/index_users"
fi
echo "Tạo index 'users'..."
curl -X PUT "http://elasticsearch:9200/index_users" -H 'Content-Type: application/json' -d @/es/users_mapping.json


