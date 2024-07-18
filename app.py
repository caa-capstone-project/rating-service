from flask import Flask, request, jsonify
from decimal import Decimal
import boto3

app = Flask(__name__)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Ratings')  # Replace with your actual DynamoDB table name

@app.route('/store_ratings', methods=['POST'])
def input_data():
    try:
        data = request.json  # Assuming data comes in as JSON
        user_id = data.get("userId")
        ratings = data.get("rating", [])

        for rating in ratings:
            movie_id = rating.get("movieId")
            rating_value = rating.get("rating")

            # print(f"userid: {user_id}, movieid: {movie_id}, rating: {rating_value}")
            decimal_rating = Decimal(str(rating_value))
            
            # Store data in DynamoDB
            table.put_item(Item={
                'userId': user_id,
                'movieId': movie_id,
                'rating': decimal_rating
            })
            
        return jsonify({'message': 'Data stored successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/test_connection', methods=['GET'])
def test_connection():
    try:
        response = table.scan(Limit=5)
        items = response.get('Items', [])
        return jsonify({'first_5_items': items}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4203)
