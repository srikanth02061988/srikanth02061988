from flask import Flask, jsonify, request

app = Flask(__name__)

'''
@app.route('/process_data', methods=['POST'])
def process_data():
    # Extracting tenant_id and usecase_id from the URL
    tenant_id = request.form.get('tenant_id', '')
    usecase_id = request.form.get('usecase_id', '')

    # process the data with tenant_id and usecase_id
    result = process_data_fun(tenant_id, usecase_id)

    return jsonify(result)


def process_data_fun(tenant_id, usecase_id):
    result = f"Processing data for Tenant Id: {tenant_id}, Usecase Id: {usecase_id}"

    return {"result": result}


if __name__ == '__main__':
    app.run(debug=True)
    # app.run(debug=True, port=5001)

'''
data_store = []


@app.route('/api', methods=['POST'])
def post_values():
    # JSON data from the requested body
    data = request.json

    # Validating if both 'tenant_id' and 'usecase_id' are provided
    if 'tenant_id' not in data or 'usecase_id' not in data:
        return jsonify(error="Both 'tenant_id' and 'usecase_id' are required."), 400

    # Store the posted values
    data_store.append(data)

    return jsonify(message="Values posted successfully.")


@app.route('/api', methods=['GET'])
def retrieve_values():
    return jsonify(data_store)


if __name__ == '__main__':
    app.run(debug=True)
