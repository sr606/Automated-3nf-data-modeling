"""
Generate 50+ diverse test files for comprehensive 3NF normalization testing
Tests various data patterns: 1NF violations, partial dependencies, transitive dependencies,
foreign keys, multivalued attributes, and edge cases
"""
import pandas as pd
import json
import random
import os
from datetime import datetime, timedelta

# Create output directory
os.makedirs('test_data_50', exist_ok=True)

# Seed for reproducibility
random.seed(42)

print("Generating 50+ test files for 3NF normalization testing...")

# ============================================================================
# CATEGORY 1: E-COMMERCE DOMAIN (10 files)
# ============================================================================

# 1. Products with multivalued attributes
products_data = []
for i in range(1, 101):
    products_data.append({
        'product_id': f'P{i:04d}',
        'product_name': f'Product {i}',
        'category_id': f'CAT{(i % 10) + 1:02d}',
        'category_name': ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 
                         'Toys', 'Food', 'Health', 'Auto', 'Garden'][i % 10],
        'supplier_id': f'SUP{(i % 20) + 1:03d}',
        'supplier_name': f'Supplier {(i % 20) + 1}',
        'supplier_city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
        'price': round(random.uniform(10, 500), 2),
        'stock': random.randint(0, 1000),
        'tags': ','.join(random.sample(['new', 'sale', 'popular', 'limited', 'organic'], k=2))
    })
pd.DataFrame(products_data).to_csv('test_data_50/01_products.csv', index=False)

# 2. Orders with transitive dependencies
orders_data = []
for i in range(1, 201):
    orders_data.append({
        'order_id': f'ORD{i:05d}',
        'customer_id': f'CUST{(i % 50) + 1:04d}',
        'customer_name': f'Customer {(i % 50) + 1}',
        'customer_email': f'customer{(i % 50) + 1}@example.com',
        'customer_city': random.choice(['Boston', 'Seattle', 'Miami', 'Denver', 'Austin']),
        'order_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
        'status_id': random.choice([1, 2, 3, 4]),
        'status_name': random.choice(['Pending', 'Processing', 'Shipped', 'Delivered']),
        'total_amount': round(random.uniform(50, 2000), 2)
    })
pd.DataFrame(orders_data).to_csv('test_data_50/02_orders.csv', index=False)

# 3. Order items (child table pattern)
order_items_data = []
item_counter = 1
for i in range(1, 201):
    num_items = random.randint(1, 5)
    for j in range(num_items):
        order_items_data.append({
            'order_item_id': f'OI{item_counter:06d}',
            'order_id': f'ORD{i:05d}',
            'product_id': f'P{random.randint(1, 100):04d}',
            'quantity': random.randint(1, 10),
            'unit_price': round(random.uniform(10, 500), 2),
            'discount': round(random.uniform(0, 0.3), 2)
        })
        item_counter += 1
pd.DataFrame(order_items_data).to_csv('test_data_50/03_order_items.csv', index=False)

# 4. Customers with address dependencies
customers_data = []
for i in range(1, 101):
    customers_data.append({
        'customer_id': f'CUST{i:04d}',
        'first_name': f'FirstName{i}',
        'last_name': f'LastName{i}',
        'email': f'customer{i}@example.com',
        'phone': f'555-{random.randint(1000, 9999)}',
        'address_id': f'ADDR{(i % 30) + 1:03d}',
        'street': f'{random.randint(100, 9999)} Main St',
        'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
        'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
        'zip': f'{random.randint(10000, 99999)}',
        'country': 'USA'
    })
pd.DataFrame(customers_data).to_csv('test_data_50/04_customers.csv', index=False)

# 5. Reviews (multi-row pattern)
reviews_data = []
for i in range(1, 301):
    reviews_data.append({
        'review_id': f'REV{i:05d}',
        'product_id': f'P{random.randint(1, 100):04d}',
        'customer_id': f'CUST{random.randint(1, 100):04d}',
        'rating': random.randint(1, 5),
        'review_text': f'Review text {i}',
        'review_date': (datetime.now() - timedelta(days=random.randint(1, 180))).strftime('%Y-%m-%d'),
        'helpful_count': random.randint(0, 100)
    })
pd.DataFrame(reviews_data).to_csv('test_data_50/05_reviews.csv', index=False)

# 6. Inventory with warehouse dependencies
inventory_data = []
for i in range(1, 151):
    inventory_data.append({
        'inventory_id': f'INV{i:05d}',
        'product_id': f'P{(i % 100) + 1:04d}',
        'warehouse_id': f'WH{(i % 5) + 1:02d}',
        'warehouse_name': f'Warehouse {(i % 5) + 1}',
        'warehouse_city': random.choice(['Dallas', 'Portland', 'Tampa', 'Atlanta', 'Detroit']),
        'quantity': random.randint(0, 500),
        'last_updated': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
    })
pd.DataFrame(inventory_data).to_csv('test_data_50/06_inventory.csv', index=False)

# 7. Promotions with product associations
promotions_data = []
for i in range(1, 31):
    promotions_data.append({
        'promotion_id': f'PROMO{i:03d}',
        'promotion_name': f'Promotion {i}',
        'discount_percent': round(random.uniform(5, 50), 0),
        'start_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'end_date': (datetime.now() + timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'product_ids': ','.join([f'P{random.randint(1, 100):04d}' for _ in range(random.randint(3, 8))])
    })
pd.DataFrame(promotions_data).to_csv('test_data_50/07_promotions.csv', index=False)

# 8. Shipping info (transitive via order)
shipping_data = []
for i in range(1, 151):
    shipping_data.append({
        'shipping_id': f'SHIP{i:05d}',
        'order_id': f'ORD{i:05d}',
        'carrier_id': f'CARR{(i % 5) + 1:02d}',
        'carrier_name': random.choice(['FedEx', 'UPS', 'USPS', 'DHL', 'Amazon']),
        'tracking_number': f'TRACK{random.randint(100000, 999999)}',
        'ship_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
        'delivery_date': (datetime.now() - timedelta(days=random.randint(1, 20))).strftime('%Y-%m-%d'),
        'shipping_cost': round(random.uniform(5, 50), 2)
    })
pd.DataFrame(shipping_data).to_csv('test_data_50/08_shipping.csv', index=False)

# 9. Payment methods
payments_data = []
for i in range(1, 201):
    payments_data.append({
        'payment_id': f'PAY{i:05d}',
        'order_id': f'ORD{i:05d}',
        'payment_method_id': random.choice([1, 2, 3, 4]),
        'payment_method_name': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
        'amount': round(random.uniform(50, 2000), 2),
        'payment_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'status': random.choice(['Completed', 'Pending', 'Failed'])
    })
pd.DataFrame(payments_data).to_csv('test_data_50/09_payments.csv', index=False)

# 10. Product categories (hierarchy)
categories_data = []
for i in range(1, 21):
    categories_data.append({
        'category_id': f'CAT{i:02d}',
        'category_name': f'Category {i}',
        'parent_category_id': f'CAT{(i // 2) if i > 1 else None:02d}' if i > 1 else None,
        'description': f'Description for category {i}'
    })
pd.DataFrame(categories_data).to_csv('test_data_50/10_categories.csv', index=False)

# ============================================================================
# CATEGORY 2: HR/EMPLOYEE DOMAIN (10 files)
# ============================================================================

# 11. Employees with department dependencies
employees_data = []
for i in range(1, 151):
    employees_data.append({
        'employee_id': f'EMP{i:05d}',
        'first_name': f'Employee{i}',
        'last_name': f'Last{i}',
        'email': f'emp{i}@company.com',
        'phone': f'555-{random.randint(1000, 9999)}',
        'department_id': f'DEPT{(i % 15) + 1:02d}',
        'department_name': random.choice(['IT', 'HR', 'Sales', 'Marketing', 'Finance', 
                                        'Operations', 'Legal', 'R&D', 'Support', 'Admin',
                                        'Product', 'Design', 'QA', 'DevOps', 'Security']),
        'department_location': random.choice(['Building A', 'Building B', 'Building C', 'Remote']),
        'job_title': random.choice(['Manager', 'Senior', 'Junior', 'Lead', 'Specialist']),
        'salary': round(random.uniform(50000, 150000), 2),
        'hire_date': (datetime.now() - timedelta(days=random.randint(30, 3650))).strftime('%Y-%m-%d')
    })
pd.DataFrame(employees_data).to_csv('test_data_50/11_employees.csv', index=False)

# 12. Employee skills (many-to-many pattern)
skills_data = []
skill_counter = 1
for i in range(1, 151):
    num_skills = random.randint(2, 6)
    for j in range(num_skills):
        skills_data.append({
            'employee_skill_id': f'ES{skill_counter:05d}',
            'employee_id': f'EMP{i:05d}',
            'skill_id': f'SKILL{random.randint(1, 30):03d}',
            'skill_name': random.choice(['Python', 'Java', 'JavaScript', 'SQL', 'React', 
                                        'AWS', 'Docker', 'Kubernetes', 'Git', 'Agile',
                                        'Leadership', 'Communication', 'Project Management',
                                        'Data Analysis', 'Machine Learning']),
            'proficiency_level': random.choice(['Beginner', 'Intermediate', 'Advanced', 'Expert']),
            'years_experience': random.randint(1, 15)
        })
        skill_counter += 1
pd.DataFrame(skills_data).to_csv('test_data_50/12_employee_skills.csv', index=False)

# 13. Projects with employee assignments
projects_data = []
for i in range(1, 51):
    projects_data.append({
        'project_id': f'PROJ{i:04d}',
        'project_name': f'Project {i}',
        'client_id': f'CLIENT{(i % 20) + 1:03d}',
        'client_name': f'Client {(i % 20) + 1}',
        'client_industry': random.choice(['Tech', 'Finance', 'Healthcare', 'Retail', 'Manufacturing']),
        'start_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
        'end_date': (datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
        'budget': round(random.uniform(50000, 500000), 2),
        'status': random.choice(['Planning', 'In Progress', 'On Hold', 'Completed', 'Cancelled'])
    })
pd.DataFrame(projects_data).to_csv('test_data_50/13_projects.csv', index=False)

# 14. Project assignments (child table)
assignments_data = []
assignment_counter = 1
for i in range(1, 51):
    num_employees = random.randint(3, 10)
    for j in range(num_employees):
        assignments_data.append({
            'assignment_id': f'ASSIGN{assignment_counter:05d}',
            'project_id': f'PROJ{i:04d}',
            'employee_id': f'EMP{random.randint(1, 150):05d}',
            'role': random.choice(['Developer', 'Designer', 'Manager', 'Analyst', 'Tester']),
            'allocation_percent': random.choice([25, 50, 75, 100]),
            'start_date': (datetime.now() - timedelta(days=random.randint(1, 180))).strftime('%Y-%m-%d')
        })
        assignment_counter += 1
pd.DataFrame(assignments_data).to_csv('test_data_50/14_project_assignments.csv', index=False)

# 15. Time tracking (event pattern)
timesheet_data = []
for i in range(1, 501):
    timesheet_data.append({
        'timesheet_id': f'TS{i:06d}',
        'employee_id': f'EMP{random.randint(1, 150):05d}',
        'project_id': f'PROJ{random.randint(1, 50):04d}',
        'work_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'hours': round(random.uniform(1, 10), 1),
        'task_description': f'Task description {i}',
        'billable': random.choice([True, False])
    })
pd.DataFrame(timesheet_data).to_csv('test_data_50/15_timesheets.csv', index=False)

# 16. Departments with location dependencies
departments_data = []
for i in range(1, 21):
    departments_data.append({
        'department_id': f'DEPT{i:02d}',
        'department_name': f'Department {i}',
        'manager_id': f'EMP{random.randint(1, 150):05d}',
        'location_id': f'LOC{(i % 5) + 1:02d}',
        'building': random.choice(['Building A', 'Building B', 'Building C', 'Building D']),
        'floor': random.randint(1, 10),
        'budget': round(random.uniform(100000, 1000000), 2)
    })
pd.DataFrame(departments_data).to_csv('test_data_50/16_departments.csv', index=False)

# 17. Performance reviews (time-series pattern)
reviews_emp_data = []
for i in range(1, 201):
    reviews_emp_data.append({
        'review_id': f'REVHR{i:05d}',
        'employee_id': f'EMP{(i % 150) + 1:05d}',
        'reviewer_id': f'EMP{random.randint(1, 150):05d}',
        'review_date': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
        'rating': random.randint(1, 5),
        'comments': f'Review comments {i}',
        'goals': f'Goals for next period {i}'
    })
pd.DataFrame(reviews_emp_data).to_csv('test_data_50/17_performance_reviews.csv', index=False)

# 18. Training courses
training_data = []
for i in range(1, 41):
    training_data.append({
        'course_id': f'COURSE{i:03d}',
        'course_name': f'Training Course {i}',
        'provider_id': f'PROV{(i % 10) + 1:02d}',
        'provider_name': f'Training Provider {(i % 10) + 1}',
        'provider_contact': f'contact{(i % 10) + 1}@training.com',
        'duration_hours': random.choice([4, 8, 16, 24, 40]),
        'cost': round(random.uniform(100, 2000), 2),
        'category': random.choice(['Technical', 'Soft Skills', 'Compliance', 'Leadership'])
    })
pd.DataFrame(training_data).to_csv('test_data_50/18_training_courses.csv', index=False)

# 19. Training enrollments (many-to-many)
enrollments_data = []
for i in range(1, 201):
    enrollments_data.append({
        'enrollment_id': f'ENROLL{i:05d}',
        'employee_id': f'EMP{random.randint(1, 150):05d}',
        'course_id': f'COURSE{random.randint(1, 40):03d}',
        'enrollment_date': (datetime.now() - timedelta(days=random.randint(1, 180))).strftime('%Y-%m-%d'),
        'completion_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d') if random.random() > 0.3 else None,
        'score': random.randint(60, 100) if random.random() > 0.3 else None,
        'status': random.choice(['Enrolled', 'In Progress', 'Completed', 'Dropped'])
    })
pd.DataFrame(enrollments_data).to_csv('test_data_50/19_training_enrollments.csv', index=False)

# 20. Benefits (employee dependencies)
benefits_data = []
for i in range(1, 151):
    benefits_data.append({
        'benefit_id': f'BEN{i:05d}',
        'employee_id': f'EMP{i:05d}',
        'plan_id': f'PLAN{random.randint(1, 10):02d}',
        'plan_name': random.choice(['Health Basic', 'Health Premium', 'Dental', 'Vision', 
                                   '401k Basic', '401k Premium', 'Life Insurance', 'Disability',
                                   'FSA', 'HSA']),
        'plan_provider': random.choice(['Provider A', 'Provider B', 'Provider C']),
        'monthly_cost': round(random.uniform(50, 500), 2),
        'enrollment_date': (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime('%Y-%m-%d')
    })
pd.DataFrame(benefits_data).to_csv('test_data_50/20_employee_benefits.csv', index=False)

# ============================================================================
# CATEGORY 3: HEALTHCARE DOMAIN (10 files)
# ============================================================================

# 21. Patients with address transitive dependencies
patients_data = []
for i in range(1, 201):
    patients_data.append({
        'patient_id': f'PT{i:06d}',
        'first_name': f'Patient{i}',
        'last_name': f'Last{i}',
        'date_of_birth': (datetime.now() - timedelta(days=random.randint(6570, 32850))).strftime('%Y-%m-%d'),
        'gender': random.choice(['M', 'F', 'Other']),
        'email': f'patient{i}@email.com',
        'phone': f'555-{random.randint(1000, 9999)}',
        'insurance_id': f'INS{(i % 30) + 1:03d}',
        'insurance_provider': random.choice(['Blue Cross', 'Aetna', 'UnitedHealth', 'Cigna', 'Humana']),
        'insurance_plan': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
        'address': f'{random.randint(100, 9999)} Health St',
        'city': random.choice(['Boston', 'Seattle', 'Miami', 'Denver', 'Portland']),
        'state': random.choice(['MA', 'WA', 'FL', 'CO', 'OR']),
        'zip': f'{random.randint(10000, 99999)}'
    })
pd.DataFrame(patients_data).to_csv('test_data_50/21_patients.csv', index=False)

# 22. Doctors with specialty dependencies
doctors_data = []
for i in range(1, 51):
    doctors_data.append({
        'doctor_id': f'DOC{i:04d}',
        'first_name': f'Dr. {i}',
        'last_name': f'Doctor{i}',
        'specialty_id': f'SPEC{(i % 15) + 1:02d}',
        'specialty_name': random.choice(['Cardiology', 'Dermatology', 'Neurology', 'Orthopedics',
                                        'Pediatrics', 'Psychiatry', 'Radiology', 'Surgery',
                                        'Family Medicine', 'Internal Medicine', 'Oncology',
                                        'Ophthalmology', 'ENT', 'Anesthesiology', 'Emergency']),
        'years_experience': random.randint(1, 40),
        'hospital_id': f'HOSP{(i % 5) + 1:02d}',
        'hospital_name': f'Hospital {(i % 5) + 1}',
        'hospital_city': random.choice(['Boston', 'New York', 'Chicago', 'LA', 'Houston']),
        'phone': f'555-{random.randint(1000, 9999)}',
        'email': f'doctor{i}@hospital.com'
    })
pd.DataFrame(doctors_data).to_csv('test_data_50/22_doctors.csv', index=False)

# 23. Appointments (child table pattern)
appointments_data = []
for i in range(1, 401):
    appointments_data.append({
        'appointment_id': f'APPT{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'doctor_id': f'DOC{random.randint(1, 50):04d}',
        'appointment_date': (datetime.now() + timedelta(days=random.randint(-30, 60))).strftime('%Y-%m-%d'),
        'appointment_time': f'{random.randint(8, 17):02d}:{random.choice(["00", "15", "30", "45"])}',
        'duration_minutes': random.choice([15, 30, 45, 60]),
        'status': random.choice(['Scheduled', 'Confirmed', 'Completed', 'Cancelled', 'No Show']),
        'reason': random.choice(['Checkup', 'Follow-up', 'New Symptoms', 'Consultation', 'Emergency'])
    })
pd.DataFrame(appointments_data).to_csv('test_data_50/23_appointments.csv', index=False)

# 24. Prescriptions with drug dependencies
prescriptions_data = []
for i in range(1, 301):
    prescriptions_data.append({
        'prescription_id': f'RX{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'doctor_id': f'DOC{random.randint(1, 50):04d}',
        'drug_id': f'DRUG{(i % 50) + 1:04d}',
        'drug_name': f'Medication {(i % 50) + 1}',
        'drug_category': random.choice(['Antibiotic', 'Pain Relief', 'Blood Pressure', 'Diabetes', 'Cholesterol']),
        'dosage': f'{random.randint(5, 500)}mg',
        'frequency': random.choice(['Once daily', 'Twice daily', 'Three times daily', 'As needed']),
        'duration_days': random.randint(7, 90),
        'date_prescribed': (datetime.now() - timedelta(days=random.randint(1, 180))).strftime('%Y-%m-%d')
    })
pd.DataFrame(prescriptions_data).to_csv('test_data_50/24_prescriptions.csv', index=False)

# 25. Lab tests with result patterns
lab_tests_data = []
for i in range(1, 251):
    lab_tests_data.append({
        'test_id': f'TEST{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'test_type_id': f'TTYPE{(i % 20) + 1:03d}',
        'test_name': random.choice(['Blood Count', 'Cholesterol', 'Glucose', 'Liver Function',
                                   'Kidney Function', 'Thyroid', 'Vitamin D', 'Iron', 'X-Ray', 'MRI']),
        'test_category': random.choice(['Blood Test', 'Imaging', 'Biopsy', 'Urine Test']),
        'order_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
        'result_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'result_value': f'{random.uniform(50, 200):.1f}',
        'normal_range': '80-120',
        'status': random.choice(['Pending', 'Completed', 'Reviewed'])
    })
pd.DataFrame(lab_tests_data).to_csv('test_data_50/25_lab_tests.csv', index=False)

# 26. Diagnoses (ICD codes)
diagnoses_data = []
for i in range(1, 201):
    diagnoses_data.append({
        'diagnosis_id': f'DX{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'doctor_id': f'DOC{random.randint(1, 50):04d}',
        'icd_code': f'ICD{random.randint(100, 999)}.{random.randint(0, 99)}',
        'diagnosis_name': f'Diagnosis {i}',
        'diagnosis_category': random.choice(['Chronic', 'Acute', 'Injury', 'Congenital', 'Infectious']),
        'diagnosis_date': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
        'severity': random.choice(['Mild', 'Moderate', 'Severe', 'Critical'])
    })
pd.DataFrame(diagnoses_data).to_csv('test_data_50/26_diagnoses.csv', index=False)

# 27. Treatments (procedure codes)
treatments_data = []
for i in range(1, 181):
    treatments_data.append({
        'treatment_id': f'TRT{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'doctor_id': f'DOC{random.randint(1, 50):04d}',
        'procedure_code': f'CPT{random.randint(10000, 99999)}',
        'procedure_name': f'Procedure {i}',
        'procedure_type': random.choice(['Surgery', 'Therapy', 'Diagnostic', 'Preventive']),
        'treatment_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
        'cost': round(random.uniform(100, 10000), 2),
        'duration_minutes': random.randint(15, 240)
    })
pd.DataFrame(treatments_data).to_csv('test_data_50/27_treatments.csv', index=False)

# 28. Medical history (time-series)
history_data = []
for i in range(1, 301):
    history_data.append({
        'history_id': f'HIST{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'record_date': (datetime.now() - timedelta(days=random.randint(1, 3650))).strftime('%Y-%m-%d'),
        'condition': random.choice(['Diabetes', 'Hypertension', 'Asthma', 'Heart Disease', 'Arthritis']),
        'status': random.choice(['Active', 'Resolved', 'Managed', 'Worsened']),
        'notes': f'Medical history notes {i}'
    })
pd.DataFrame(history_data).to_csv('test_data_50/28_medical_history.csv', index=False)

# 29. Billing (transitive dependencies)
billing_data = []
for i in range(1, 251):
    billing_data.append({
        'bill_id': f'BILL{i:06d}',
        'patient_id': f'PT{random.randint(1, 200):06d}',
        'appointment_id': f'APPT{random.randint(1, 400):06d}',
        'insurance_id': f'INS{random.randint(1, 30):03d}',
        'insurance_provider': random.choice(['Blue Cross', 'Aetna', 'UnitedHealth', 'Cigna', 'Humana']),
        'total_amount': round(random.uniform(100, 5000), 2),
        'insurance_covered': round(random.uniform(50, 4000), 2),
        'patient_responsibility': round(random.uniform(10, 1000), 2),
        'bill_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
        'payment_status': random.choice(['Pending', 'Partially Paid', 'Paid', 'Overdue'])
    })
pd.DataFrame(billing_data).to_csv('test_data_50/29_billing.csv', index=False)

# 30. Hospital departments
hospital_depts_data = []
for i in range(1, 26):
    hospital_depts_data.append({
        'department_id': f'HDEPT{i:03d}',
        'department_name': random.choice(['Emergency', 'ICU', 'Surgery', 'Maternity', 'Pediatrics',
                                         'Cardiology', 'Neurology', 'Oncology', 'Radiology', 'Laboratory']),
        'hospital_id': f'HOSP{(i % 5) + 1:02d}',
        'hospital_name': f'Hospital {(i % 5) + 1}',
        'building': random.choice(['Main', 'North Wing', 'South Wing', 'East Tower']),
        'floor': random.randint(1, 10),
        'beds': random.randint(10, 100)
    })
pd.DataFrame(hospital_depts_data).to_csv('test_data_50/30_hospital_departments.csv', index=False)

# ============================================================================
# CATEGORY 4: EDUCATION DOMAIN (10 files)
# ============================================================================

# 31. Students with address dependencies
students_data = []
for i in range(1, 301):
    students_data.append({
        'student_id': f'STU{i:06d}',
        'first_name': f'Student{i}',
        'last_name': f'Last{i}',
        'email': f'student{i}@university.edu',
        'date_of_birth': (datetime.now() - timedelta(days=random.randint(6570, 10950))).strftime('%Y-%m-%d'),
        'enrollment_date': (datetime.now() - timedelta(days=random.randint(30, 1460))).strftime('%Y-%m-%d'),
        'major_id': f'MAJOR{(i % 20) + 1:03d}',
        'major_name': random.choice(['Computer Science', 'Business', 'Engineering', 'Biology',
                                    'Psychology', 'English', 'Mathematics', 'History', 'Art', 'Music']),
        'department': random.choice(['Science', 'Arts', 'Engineering', 'Business', 'Humanities']),
        'gpa': round(random.uniform(2.0, 4.0), 2),
        'address': f'{random.randint(100, 9999)} Campus Dr',
        'city': random.choice(['Berkeley', 'Cambridge', 'Ann Arbor', 'Chapel Hill', 'Madison']),
        'state': random.choice(['CA', 'MA', 'MI', 'NC', 'WI'])
    })
pd.DataFrame(students_data).to_csv('test_data_50/31_students.csv', index=False)

# 32. Courses with instructor dependencies
courses_data = []
for i in range(1, 81):
    courses_data.append({
        'course_id': f'COURSE{i:04d}',
        'course_code': f'CS{random.randint(100, 599)}',
        'course_name': f'Course {i}',
        'instructor_id': f'INST{(i % 30) + 1:03d}',
        'instructor_name': f'Professor {(i % 30) + 1}',
        'instructor_email': f'prof{(i % 30) + 1}@university.edu',
        'department_id': f'DEPT{(i % 10) + 1:02d}',
        'department_name': random.choice(['Computer Science', 'Mathematics', 'Physics', 'Chemistry',
                                         'Biology', 'Engineering', 'Business', 'Economics', 'Psychology', 'Philosophy']),
        'credits': random.choice([3, 4, 5]),
        'semester': random.choice(['Fall 2024', 'Spring 2025', 'Summer 2025']),
        'max_enrollment': random.randint(20, 100)
    })
pd.DataFrame(courses_data).to_csv('test_data_50/32_courses.csv', index=False)

# 33. Enrollments (many-to-many)
course_enrollments_data = []
enrollment_counter = 1
for i in range(1, 301):
    num_courses = random.randint(3, 6)
    for j in range(num_courses):
        course_enrollments_data.append({
            'enrollment_id': f'ENR{enrollment_counter:06d}',
            'student_id': f'STU{i:06d}',
            'course_id': f'COURSE{random.randint(1, 80):04d}',
            'semester': random.choice(['Fall 2024', 'Spring 2025']),
            'enrollment_date': (datetime.now() - timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
            'status': random.choice(['Active', 'Completed', 'Withdrawn', 'Incomplete'])
        })
        enrollment_counter += 1
pd.DataFrame(course_enrollments_data).to_csv('test_data_50/33_course_enrollments.csv', index=False)

# 34. Grades (child of enrollments)
grades_data = []
for i in range(1, 801):
    grades_data.append({
        'grade_id': f'GRD{i:06d}',
        'enrollment_id': f'ENR{i:06d}',
        'assignment_type': random.choice(['Homework', 'Quiz', 'Midterm', 'Final', 'Project']),
        'assignment_name': f'Assignment {random.randint(1, 20)}',
        'score': round(random.uniform(60, 100), 1),
        'max_score': 100,
        'weight': random.choice([0.1, 0.15, 0.2, 0.25, 0.3]),
        'date': (datetime.now() - timedelta(days=random.randint(1, 120))).strftime('%Y-%m-%d')
    })
pd.DataFrame(grades_data).to_csv('test_data_50/34_grades.csv', index=False)

# 35. Faculty with department dependencies
faculty_data = []
for i in range(1, 61):
    faculty_data.append({
        'faculty_id': f'FAC{i:04d}',
        'first_name': f'Professor{i}',
        'last_name': f'Last{i}',
        'email': f'faculty{i}@university.edu',
        'phone': f'555-{random.randint(1000, 9999)}',
        'department_id': f'DEPT{(i % 10) + 1:02d}',
        'department_name': random.choice(['Computer Science', 'Mathematics', 'Physics', 'Chemistry',
                                         'Biology', 'Engineering', 'Business', 'Economics', 'Psychology', 'Philosophy']),
        'office_building': random.choice(['Science Hall', 'Engineering Building', 'Arts Center', 'Business School']),
        'office_number': f'{random.randint(100, 999)}',
        'rank': random.choice(['Assistant Professor', 'Associate Professor', 'Professor', 'Lecturer']),
        'hire_date': (datetime.now() - timedelta(days=random.randint(365, 10950))).strftime('%Y-%m-%d'),
        'salary': round(random.uniform(60000, 150000), 2)
    })
pd.DataFrame(faculty_data).to_csv('test_data_50/35_faculty.csv', index=False)

# 36. Classrooms with building dependencies
classrooms_data = []
for i in range(1, 51):
    classrooms_data.append({
        'classroom_id': f'ROOM{i:04d}',
        'room_number': f'{random.randint(100, 999)}',
        'building_id': f'BLDG{(i % 10) + 1:02d}',
        'building_name': random.choice(['Science Hall', 'Engineering Building', 'Arts Center', 
                                       'Business School', 'Library', 'Student Union', 'Athletic Center',
                                       'Medical Building', 'Law Building', 'Music Hall']),
        'building_address': f'{random.randint(100, 999)} Campus Way',
        'capacity': random.randint(20, 200),
        'room_type': random.choice(['Lecture Hall', 'Seminar Room', 'Lab', 'Computer Lab', 'Studio']),
        'has_projector': random.choice([True, False]),
        'has_computers': random.choice([True, False])
    })
pd.DataFrame(classrooms_data).to_csv('test_data_50/36_classrooms.csv', index=False)

# 37. Class schedules (composite pattern)
schedules_data = []
for i in range(1, 121):
    schedules_data.append({
        'schedule_id': f'SCH{i:05d}',
        'course_id': f'COURSE{(i % 80) + 1:04d}',
        'classroom_id': f'ROOM{random.randint(1, 50):04d}',
        'day_of_week': random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']),
        'start_time': f'{random.randint(8, 17):02d}:00',
        'end_time': f'{random.randint(9, 18):02d}:00',
        'semester': random.choice(['Fall 2024', 'Spring 2025'])
    })
pd.DataFrame(schedules_data).to_csv('test_data_50/37_class_schedules.csv', index=False)

# 38. Attendance (time-series)
attendance_data = []
for i in range(1, 501):
    attendance_data.append({
        'attendance_id': f'ATT{i:06d}',
        'enrollment_id': f'ENR{random.randint(1, 800):06d}',
        'class_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
        'status': random.choice(['Present', 'Absent', 'Excused', 'Late']),
        'notes': f'Notes {i}' if random.random() > 0.8 else None
    })
pd.DataFrame(attendance_data).to_csv('test_data_50/38_attendance.csv', index=False)

# 39. Library books with author dependencies
books_data = []
for i in range(1, 201):
    books_data.append({
        'book_id': f'BOOK{i:05d}',
        'isbn': f'978-{random.randint(1000000000, 9999999999)}',
        'title': f'Book Title {i}',
        'author_id': f'AUTH{(i % 50) + 1:04d}',
        'author_name': f'Author {(i % 50) + 1}',
        'author_country': random.choice(['USA', 'UK', 'Canada', 'France', 'Germany']),
        'publisher': random.choice(['Publisher A', 'Publisher B', 'Publisher C', 'Publisher D']),
        'publication_year': random.randint(1990, 2024),
        'category': random.choice(['Fiction', 'Non-Fiction', 'Science', 'History', 'Biography']),
        'available_copies': random.randint(0, 10)
    })
pd.DataFrame(books_data).to_csv('test_data_50/39_library_books.csv', index=False)

# 40. Book checkouts (event pattern)
checkouts_data = []
for i in range(1, 301):
    checkouts_data.append({
        'checkout_id': f'CHKOUT{i:06d}',
        'book_id': f'BOOK{random.randint(1, 200):05d}',
        'student_id': f'STU{random.randint(1, 300):06d}',
        'checkout_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'due_date': (datetime.now() + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
        'return_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d') if random.random() > 0.4 else None,
        'status': random.choice(['Checked Out', 'Returned', 'Overdue', 'Lost'])
    })
pd.DataFrame(checkouts_data).to_csv('test_data_50/40_book_checkouts.csv', index=False)

# ============================================================================
# CATEGORY 5: FINANCIAL/BANKING DOMAIN (10 files)
# ============================================================================

# 41. Bank customers with address transitive deps
bank_customers_data = []
for i in range(1, 201):
    bank_customers_data.append({
        'customer_id': f'BC{i:06d}',
        'first_name': f'Customer{i}',
        'last_name': f'Last{i}',
        'ssn': f'{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}',
        'email': f'customer{i}@bank.com',
        'phone': f'555-{random.randint(1000, 9999)}',
        'branch_id': f'BRANCH{(i % 15) + 1:03d}',
        'branch_name': f'Branch {(i % 15) + 1}',
        'branch_city': random.choice(['New York', 'San Francisco', 'Chicago', 'Boston', 'Miami']),
        'branch_state': random.choice(['NY', 'CA', 'IL', 'MA', 'FL']),
        'customer_since': (datetime.now() - timedelta(days=random.randint(365, 7300))).strftime('%Y-%m-%d'),
        'customer_type': random.choice(['Individual', 'Business', 'Corporate'])
    })
pd.DataFrame(bank_customers_data).to_csv('test_data_50/41_bank_customers.csv', index=False)

# 42. Accounts (one-to-many with customer)
accounts_data = []
for i in range(1, 401):
    accounts_data.append({
        'account_id': f'ACC{i:08d}',
        'customer_id': f'BC{random.randint(1, 200):06d}',
        'account_type_id': random.choice([1, 2, 3, 4, 5]),
        'account_type': random.choice(['Checking', 'Savings', 'Money Market', 'CD', 'Investment']),
        'account_number': f'{random.randint(100000000, 999999999)}',
        'balance': round(random.uniform(100, 100000), 2),
        'interest_rate': round(random.uniform(0.01, 5.0), 2),
        'opened_date': (datetime.now() - timedelta(days=random.randint(30, 3650))).strftime('%Y-%m-%d'),
        'status': random.choice(['Active', 'Closed', 'Frozen', 'Dormant'])
    })
pd.DataFrame(accounts_data).to_csv('test_data_50/42_bank_accounts.csv', index=False)

# 43. Transactions (high volume event pattern)
transactions_data = []
for i in range(1, 1001):
    transactions_data.append({
        'transaction_id': f'TXN{i:010d}',
        'account_id': f'ACC{random.randint(1, 400):08d}',
        'transaction_type': random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'Fee']),
        'amount': round(random.uniform(10, 5000), 2),
        'transaction_date': (datetime.now() - timedelta(days=random.randint(1, 180))).strftime('%Y-%m-%d'),
        'transaction_time': f'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}',
        'description': f'Transaction {i}',
        'balance_after': round(random.uniform(100, 100000), 2),
        'status': random.choice(['Posted', 'Pending', 'Failed', 'Reversed'])
    })
pd.DataFrame(transactions_data).to_csv('test_data_50/43_transactions.csv', index=False)

# 44. Loans with customer dependencies
loans_data = []
for i in range(1, 151):
    loans_data.append({
        'loan_id': f'LOAN{i:06d}',
        'customer_id': f'BC{random.randint(1, 200):06d}',
        'loan_type_id': random.choice([1, 2, 3, 4, 5]),
        'loan_type': random.choice(['Mortgage', 'Auto', 'Personal', 'Student', 'Business']),
        'loan_officer_id': f'LO{(i % 20) + 1:03d}',
        'loan_officer_name': f'Loan Officer {(i % 20) + 1}',
        'principal_amount': round(random.uniform(5000, 500000), 2),
        'interest_rate': round(random.uniform(3.0, 12.0), 2),
        'term_months': random.choice([12, 24, 36, 60, 120, 180, 360]),
        'monthly_payment': round(random.uniform(100, 5000), 2),
        'start_date': (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime('%Y-%m-%d'),
        'status': random.choice(['Active', 'Paid Off', 'Defaulted', 'Delinquent'])
    })
pd.DataFrame(loans_data).to_csv('test_data_50/44_loans.csv', index=False)

# 45. Loan payments (child of loans)
loan_payments_data = []
payment_counter = 1
for i in range(1, 151):
    num_payments = random.randint(1, 24)
    for j in range(num_payments):
        loan_payments_data.append({
            'payment_id': f'LPAY{payment_counter:08d}',
            'loan_id': f'LOAN{i:06d}',
            'payment_date': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
            'amount': round(random.uniform(100, 5000), 2),
            'principal_paid': round(random.uniform(50, 4000), 2),
            'interest_paid': round(random.uniform(10, 1000), 2),
            'remaining_balance': round(random.uniform(1000, 400000), 2),
            'status': random.choice(['Posted', 'Pending', 'Late', 'Returned'])
        })
        payment_counter += 1
pd.DataFrame(loan_payments_data).to_csv('test_data_50/45_loan_payments.csv', index=False)

# 46. Credit cards
credit_cards_data = []
for i in range(1, 251):
    credit_cards_data.append({
        'card_id': f'CC{i:08d}',
        'customer_id': f'BC{random.randint(1, 200):06d}',
        'card_number': f'{random.randint(1000, 9999)}-****-****-{random.randint(1000, 9999)}',
        'card_type': random.choice(['Visa', 'Mastercard', 'Amex', 'Discover']),
        'credit_limit': round(random.uniform(1000, 50000), 2),
        'current_balance': round(random.uniform(0, 30000), 2),
        'available_credit': round(random.uniform(0, 50000), 2),
        'interest_rate': round(random.uniform(12.0, 24.0), 2),
        'issue_date': (datetime.now() - timedelta(days=random.randint(365, 2920))).strftime('%Y-%m-%d'),
        'expiration_date': (datetime.now() + timedelta(days=random.randint(365, 1460))).strftime('%Y-%m-%d'),
        'status': random.choice(['Active', 'Closed', 'Suspended', 'Expired'])
    })
pd.DataFrame(credit_cards_data).to_csv('test_data_50/46_credit_cards.csv', index=False)

# 47. Credit card transactions
cc_transactions_data = []
for i in range(1, 601):
    cc_transactions_data.append({
        'transaction_id': f'CCTXN{i:010d}',
        'card_id': f'CC{random.randint(1, 250):08d}',
        'merchant_id': f'MERCH{(i % 100) + 1:05d}',
        'merchant_name': f'Merchant {(i % 100) + 1}',
        'merchant_category': random.choice(['Retail', 'Grocery', 'Restaurant', 'Gas', 'Online', 'Travel']),
        'amount': round(random.uniform(5, 1000), 2),
        'transaction_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
        'transaction_time': f'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}',
        'status': random.choice(['Posted', 'Pending', 'Declined', 'Refunded'])
    })
pd.DataFrame(cc_transactions_data).to_csv('test_data_50/47_credit_card_transactions.csv', index=False)

# 48. Investment portfolios
portfolios_data = []
for i in range(1, 101):
    portfolios_data.append({
        'portfolio_id': f'PORT{i:05d}',
        'customer_id': f'BC{random.randint(1, 200):06d}',
        'account_id': f'ACC{random.randint(1, 400):08d}',
        'portfolio_type': random.choice(['Retirement', 'Brokerage', '529 Plan', 'Trust']),
        'advisor_id': f'ADV{(i % 15) + 1:03d}',
        'advisor_name': f'Advisor {(i % 15) + 1}',
        'total_value': round(random.uniform(10000, 1000000), 2),
        'created_date': (datetime.now() - timedelta(days=random.randint(365, 3650))).strftime('%Y-%m-%d'),
        'risk_level': random.choice(['Conservative', 'Moderate', 'Aggressive', 'Very Aggressive'])
    })
pd.DataFrame(portfolios_data).to_csv('test_data_50/48_investment_portfolios.csv', index=False)

# 49. Securities holdings (child of portfolios)
holdings_data = []
holding_counter = 1
for i in range(1, 101):
    num_holdings = random.randint(5, 20)
    for j in range(num_holdings):
        holdings_data.append({
            'holding_id': f'HOLD{holding_counter:06d}',
            'portfolio_id': f'PORT{i:05d}',
            'security_id': f'SEC{random.randint(1, 100):05d}',
            'security_symbol': f'{chr(random.randint(65, 90))}{chr(random.randint(65, 90))}{chr(random.randint(65, 90))}',
            'security_name': f'Security {random.randint(1, 100)}',
            'security_type': random.choice(['Stock', 'Bond', 'Mutual Fund', 'ETF', 'Option']),
            'shares': round(random.uniform(1, 1000), 2),
            'purchase_price': round(random.uniform(10, 500), 2),
            'current_price': round(random.uniform(10, 500), 2),
            'purchase_date': (datetime.now() - timedelta(days=random.randint(1, 1825))).strftime('%Y-%m-%d')
        })
        holding_counter += 1
pd.DataFrame(holdings_data).to_csv('test_data_50/49_securities_holdings.csv', index=False)

# 50. ATM transactions with location dependencies
atm_transactions_data = []
for i in range(1, 401):
    atm_transactions_data.append({
        'atm_transaction_id': f'ATM{i:08d}',
        'account_id': f'ACC{random.randint(1, 400):08d}',
        'atm_id': f'ATM{(i % 50) + 1:04d}',
        'atm_location': random.choice(['Downtown', 'Airport', 'Mall', 'Branch', 'Gas Station']),
        'atm_address': f'{random.randint(100, 9999)} Main St',
        'atm_city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
        'transaction_type': random.choice(['Withdrawal', 'Deposit', 'Balance Inquiry', 'Transfer']),
        'amount': round(random.uniform(20, 500), 2),
        'fee': round(random.uniform(0, 3.5), 2),
        'transaction_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
        'transaction_time': f'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}',
        'status': random.choice(['Completed', 'Failed', 'Cancelled'])
    })
pd.DataFrame(atm_transactions_data).to_csv('test_data_50/50_atm_transactions.csv', index=False)

print("\n✅ Successfully generated 50 test files in 'test_data_50/' directory!")
print("\nFile distribution:")
print("  - E-commerce: 10 files (products, orders, customers, reviews, inventory, etc.)")
print("  - HR/Employee: 10 files (employees, skills, projects, timesheets, benefits, etc.)")
print("  - Healthcare: 10 files (patients, doctors, appointments, prescriptions, billing, etc.)")
print("  - Education: 10 files (students, courses, enrollments, grades, library, etc.)")
print("  - Financial: 10 files (customers, accounts, transactions, loans, investments, etc.)")
print("\nData patterns included:")
print("  ✓ Transitive dependencies")
print("  ✓ Partial dependencies (composite keys)")
print("  ✓ Multivalued attributes")
print("  ✓ Foreign key relationships")
print("  ✓ Child table patterns (1-to-many)")
print("  ✓ Many-to-many relationships")
print("  ✓ Time-series/event patterns")
print("  ✓ High cardinality and low cardinality columns")
print("\nTotal records generated: ~15,000+ rows across all files")
print("\nReady for 3NF normalization testing!")
