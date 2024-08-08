from flask import Flask, request, jsonify
from decimal import Decimal
from flask_cors import CORS
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

app = Flask(__name__)
CORS(app)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Ratings')  # Replace with your actual DynamoDB table name

@app.route('/rating', methods=['POST'])
def input_data():
    try:
        data = request.get_json()  # Assuming data comes in as JSON
        print("Request: ", data)
        userId = data.get("userId")
        ratings = data.get("ratings", [])

        for rating in ratings:
            print("Rating: ", rating)
            rating_value = rating.get("rating")
            rating_value = Decimal(str(rating_value))
            rating["rating"] = rating_value

        table.put_item(Item={
                'userId': userId,
                'ratings': ratings
            })
            
        return jsonify({'message': 'Data stored successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/rating/<string:userId>', methods=['GET'])
def getRating(userId):
    try:
        userId = int(userId)  # Convert userId to a number

        response = table.get_item(
            Key={
                'userId': userId
            }
        )
        
        if 'Item' in response:
            return jsonify(response['Item']), 200
        else:
            return jsonify({'message': 'Rating not found'}), 404

    except ValueError:
        return jsonify({'error': 'Invalid userId format'}), 400
    except NoCredentialsError:
        return jsonify({'error': 'AWS credentials not found'}), 500
    except PartialCredentialsError:
        return jsonify({'error': 'Incomplete AWS credentials'}), 500
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
