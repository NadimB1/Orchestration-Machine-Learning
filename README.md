# orchestration

# Test Fastapi

After every changement in the image fastapi run these cmds:
    docker build -t test_api .
    docker container run -p 8000:8000 test_api