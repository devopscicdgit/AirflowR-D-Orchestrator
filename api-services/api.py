from flask import Flask, jsonify, request
import csv
import os

DATA_DIR = './data'  # Update to your data folder

app = Flask(__name__)

# --- Utility function to load CSV data into dict keyed by a specific column ---
def load_data(file_path, key):
    data = {}
    if not os.path.exists(file_path):
        return data
    with open(file_path, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            data[row[key]] = row
    return data

# --- Load datasets ---
suppliers = load_data(f'{DATA_DIR}/suppliers.csv', 'supplier_id')
materials = load_data(f'{DATA_DIR}/materials.csv', 'material_id')
experiments = load_data(f'{DATA_DIR}/experiments.csv', 'experiment_id')
orders = load_data(f'{DATA_DIR}/orders.csv', 'order_id')

# --- API Endpoints ---

# Suppliers
@app.route('/api/suppliers', methods=['GET'])
def get_suppliers():
    return jsonify(list(suppliers.values()))

@app.route('/api/suppliers/<supplier_id>', methods=['GET'])
def get_supplier_by_id(supplier_id):
    supplier = suppliers.get(supplier_id)
    if supplier:
        return jsonify(supplier)
    return jsonify({'error': 'Supplier not found'}), 404

# Materials
@app.route('/api/materials', methods=['GET'])
def get_materials():
    supplier_id = request.args.get('supplier_id')
    if supplier_id:
        filtered = [m for m in materials.values() if m['supplier_id'] == supplier_id]
        return jsonify(filtered)
    return jsonify(list(materials.values()))

@app.route('/api/materials/<material_id>', methods=['GET'])
def get_material_by_id(material_id):
    material = materials.get(material_id)
    if material:
        return jsonify(material)
    return jsonify({'error': 'Material not found'}), 404

# Experiments
@app.route('/api/experiments', methods=['GET'])
def get_experiments():
    material_id = request.args.get('material_id')
    if material_id:
        filtered = [e for e in experiments.values() if e['material_id'] == material_id]
        return jsonify(filtered)
    return jsonify(list(experiments.values()))

@app.route('/api/experiments/<experiment_id>', methods=['GET'])
def get_experiment_by_id(experiment_id):
    experiment = experiments.get(experiment_id)
    if experiment:
        return jsonify(experiment)
    return jsonify({'error': 'Experiment not found'}), 404

# Orders
@app.route('/api/orders', methods=['GET'])
def get_orders():
    supplier_id = request.args.get('supplier_id')
    material_id = request.args.get('material_id')
    filtered_orders = list(orders.values())

    if supplier_id:
        filtered_orders = [o for o in filtered_orders if o['supplier_id'] == supplier_id]
    if material_id:
        filtered_orders = [o for o in filtered_orders if o['material_id'] == material_id]

    return jsonify(filtered_orders)

@app.route('/api/orders/<order_id>', methods=['GET'])
def get_order_by_id(order_id):
    order = orders.get(order_id)
    if order:
        return jsonify(order)
    return jsonify({'error': 'Order not found'}), 404

# --- Run the app ---
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
