setup() {
  docker compose -f integration_tests/compose_files/milvus_to_qdrant.yaml up -d --wait
}

teardown() {
  docker compose -f integration_tests/compose_files/milvus_to_qdrant.yaml down -v
}

@test "Upload vectors to Milvus and migrate to Qdrant" {
  run curl --request POST \
  --url "http://localhost:19530/v2/vectordb/collections/create" \
  --header "Content-Type: application/json" \
  -d '{
      "collectionName": "migrate_collection",
      "dimension": 5
  }'
  [ "$status" -eq 0 ]

  run curl --request POST \
  --url "http://localhost:19530/v2/vectordb/entities/insert" \
  --header "Content-Type: application/json" \
  -d '{
      "data": [
        {"id": 0, "vector": [0.358, -0.602, 0.184, -0.263, 0.903], "color": "pink_8682"},
        {"id": 1, "vector": [0.199, 0.060, 0.698, 0.261, 0.839], "color": "red_7025"},
        {"id": 2, "vector": [0.437, -0.560, 0.646, 0.789, 0.208], "color": "orange_6781"},
        {"id": 3, "vector": [0.317, 0.972, -0.370, -0.486, 0.958], "color": "pink_9298"},
        {"id": 4, "vector": [0.445, -0.876, 0.822, 0.464, 0.303], "color": "red_4794"},
        {"id": 5, "vector": [0.986, -0.814, 0.630, 0.121, -0.145], "color": "yellow_4222"},
        {"id": 6, "vector": [0.837, -0.016, -0.311, -0.563, -0.898], "color": "red_9392"},
        {"id": 7, "vector": [-0.334, -0.257, 0.899, 0.940, 0.538], "color": "grey_8510"},
        {"id": 8, "vector": [0.395, 0.400, -0.589, -0.865, -0.614], "color": "white_9381"},
        {"id": 9, "vector": [0.572, 0.241, -0.374, -0.067, -0.698], "color": "purple_4976"}
      ],
      "collectionName": "migrate_collection"
  }'
  [ "$status" -eq 0 ]


  run curl --request POST \
  --url "http://localhost:19530/v2/vectordb/collections/load" \
  --header "Content-Type: application/json" \
  -d '{
      "collectionName": "migrate_collection"
  }'
  [ "$status" -eq 0 ]

  echo "Wait for a few seconds to load the vectors"
  sleep 5

  run docker run --net=host --rm registry.cloud.qdrant.io/library/qdrant-migration:dev milvus \
    --milvus.url 'http://localhost:19530' \
    --milvus.collection 'migrate_collection' \
    --qdrant.url 'http://localhost:6334' \
    --qdrant.collection 'target-collection' \
    --migration.create-collection \
    --migration.batch-size 10
  [ "$status" -eq 0 ]

  run curl --silent --show-error --fail --request POST \
    --url "http://localhost:6333/collections/target-collection/points/count" \
    --header 'Content-Type: application/json' \
    --data '{"exact": true}'
  [ "$status" -eq 0 ]
  [[ "$output" == *'"count":10'* ]]

}
