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

  run docker run --net=host --rm registry.cloud.qdrant.io/library/qdrant-migration:dev qdrant --source.url http://localhost:7334 --source.collection source_collection --target.url http://localhost:8334 --target.collection target_collection --migration.batch-size 1
  [ $status -eq 0 ]

  run curl -s -X POST http://localhost:8333/collections/target_collection/points/scroll \
    -H 'Content-Type: application/json' \
    --data-raw '{}'
  [ $status -eq 0 ]

  target_result=$(echo "$output" | jq -r '.result')

  echo $target_result

  [ "$source_result" = "$target_result" ]
}

@test "Migrating to the same collection should fail" {
  run curl -X PUT http://localhost:7333/collections/source_collection \
    -H 'Content-Type: application/json' \
    --data-raw '{
      "vectors": {
        "size": 3,
        "distance": "Cosine"
      }
    }'
  [ $status -eq 0 ]


  run docker run --net=host --rm registry.cloud.qdrant.io/library/qdrant-migration:dev qdrant --source.url http://localhost:7334 --source.collection source_collection --target.url http://localhost:7334 --target.collection source_collection --migration.batch-size 1
  [ $status -ne 0 ]
  [[ "$output" =~ "source and target collections must be different" ]]
}

@test "Migrating with invalid port should fail" {
  run docker run --net=host --rm registry.cloud.qdrant.io/library/qdrant-migration:dev drant --source.url http://localhost:invalid --source.collection source_collection --target.url http://localhost:8334 --target.collection source_collection --migration.batch-size 1
  [ $status -ne 0 ]
  [[ "$output" =~ "invalid port \":invalid\" after host" ]]
}

setup() {
  docker compose -f integration_tests/compose_files/qdrant_to_qdrant.yaml up -d --wait
}

teardown() {
  docker compose -f integration_tests/compose_files/qdrant_to_qdrant.yaml down -v
}