@test "Migrate Qdrant to Qdrant" {
  run curl -X PUT http://localhost:7333/collections/source_collection \
    -H 'Content-Type: application/json' \
    --data-raw '{
      "vectors": {
        "size": 3,
        "distance": "Cosine"
      }
    }'
  [ $status -eq 0 ]

  run curl -X PUT http://localhost:8333/collections/target_collection \
    -H 'Content-Type: application/json' \
    --data-raw '{
      "vectors": {
        "size": 3,
        "distance": "Cosine"
      }
    }'
  [ $status -eq 0 ]

  run curl -X PUT "http://localhost:7333/collections/source_collection/points?wait=true" \
    -H 'Content-Type: application/json' \
    --data-raw '{
      "points": [
          {
              "id": 1,
              "payload": {"color": "red"},
              "vector": [0.9, 0.1, 0.1]
          },
          {
              "id": 2,
              "payload": {"color": "blue"},
              "vector": [0.9, 0.1, 0.2]
          }
      ]
    }'
  [ $status -eq 0 ]

  run curl -s -X POST http://localhost:7333/collections/source_collection/points/scroll \
    -H 'Content-Type: application/json' \
    --data-raw '{}'
  [ $status -eq 0 ]

  echo $output
  source_result=$(echo "$output" | jq -r '.result')

  echo $source_result

  run go run main.go migrate --source-url http://localhost:7334 --source-collection source_collection --target-url http://localhost:8334 --target-collection target_collection --batch-size 1
  [ $status -eq 0 ]

  run curl -s -X POST http://localhost:8333/collections/target_collection/points/scroll \
    -H 'Content-Type: application/json' \
    --data-raw '{}'
  [ $status -eq 0 ]

  target_result=$(echo "$output" | jq -r '.result')

  echo $target_result

  [ "$source_result" = "$target_result" ]
}

setup() {
  docker compose -f integration_tests/compose_files/qdrant_to_qdrant.yaml up -d --wait
}

teardown() {
  docker compose -f integration_tests/compose_files/qdrant_to_qdrant.yaml down -v
}