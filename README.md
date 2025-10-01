# Rating API

This is a Flask application that provides a simple API for storing user ratings in a DynamoDB table.

## Step to run
1. Create virtual environment \
`python3 -m venv venv`

2. Activate virtual environment
- Windows: `venv\Scripts\activate`
- Mac and Linux: `source venv/bin/activate`

3. Install Packages \
`pip install -r ./requirements.txt`

4. Run app \
`python app.py`

# Build and push image for both arm64 and amd64 arch
`docker buildx build --platform linux/amd64,linux/arm64 -t trystan00000/rating-service:latest --push .`