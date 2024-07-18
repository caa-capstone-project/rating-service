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
        for user_id, ratings in data.items():
            for movie_id, rating in ratings:
                print(f"userid: {type(user_id)},movieid: {type(movie_id)}, rating: {type(rating)}")
                decimal_rating = Decimal(str(rating))
                # Store data in DynamoDB
                table.put_item(Item={
                    'userId': int(user_id),
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
    app.run(debug=True, host='0.0.0.0', port=5000)
