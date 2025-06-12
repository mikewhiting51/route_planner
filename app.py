import os
import uuid
import sqlite3
import json
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
from flask import (
    Flask, request, render_template, redirect, url_for,
    session, send_file, jsonify, flash, g
)
from flask_login import (
    LoginManager, UserMixin,
    login_user, login_required, logout_user, current_user
)
from werkzeug.security import generate_password_hash, check_password_hash

# ----------------------
# Flask application setup
# ----------------------
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Database file path
DATABASE = os.path.join(app.root_path, 'schedules.db')

# Flask-Login setup
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)

# In-memory store for immediate session data, keyed by user_id
# Format:
# data_store[user_id] = {
#     'appointments': [...],          # for specific-date definitions
#     'config': {'date_start':..., 'date_end':..., 'dates':[...]}   # for specific-date
# }
# data_store[user_id]['recurring'] = {
#     'appointments': [...],          # for recurring definitions
#     'patterns': [...]               # list of {'label': 'First Monday', 'frequency':..., 'day':...}
# }
data_store: dict = {}

# Fixed truck names and slot definitions
FIXED_TRUCKS = [
    "Trailer 1", "Trailer 2", "Trailer 3", "Trailer 4",
    "Straight 1", "Straight 2", "Straight 3", "Straight 4"
]
SLOT_DEFINITIONS = [
    {"label": "A", "start_hour": 7.0, "end_hour": 11.0},
    {"label": "B", "start_hour": 11.0, "end_hour": 14.0}
]

# ----------------------
# SQLite helpers
# ----------------------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
        # If you need foreign keys: 
        # db.execute('PRAGMA foreign_keys = ON')
        g._database = db
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    """Initialize tables: users, specific_definitions, recurring_definitions,
       specific_schedules, recurring_schedules."""
    db = sqlite3.connect(DATABASE)
    cursor = db.cursor()
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
    ''')
    # Definitions tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS specific_definitions (
            user_id INTEGER PRIMARY KEY,
            data TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recurring_definitions (
            user_id INTEGER PRIMARY KEY,
            data TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')
    # Schedules tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS specific_schedules (
            user_id INTEGER PRIMARY KEY,
            data TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recurring_schedules (
            user_id INTEGER PRIMARY KEY,
            data TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    ''')
    db.commit()
    db.close()

with app.app_context():
    init_db()

# Helpers for persisted definitions and schedules
def save_specific_definitions_to_db(user_id, data_json_str):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO specific_definitions(user_id, data)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET data=excluded.data
    ''', (user_id, data_json_str))
    db.commit()

def load_specific_definitions_from_db(user_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT data FROM specific_definitions WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    return row['data'] if row else None

def save_recurring_definitions_to_db(user_id, data_json_str):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO recurring_definitions(user_id, data)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET data=excluded.data
    ''', (user_id, data_json_str))
    db.commit()

def load_recurring_definitions_from_db(user_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT data FROM recurring_definitions WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    return row['data'] if row else None

def save_specific_schedule_to_db(user_id, data_json_str):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO specific_schedules(user_id, data)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET data=excluded.data
    ''', (user_id, data_json_str))
    db.commit()

def load_specific_schedule_from_db(user_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT data FROM specific_schedules WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    return row['data'] if row else None

def save_recurring_schedule_to_db(user_id, data_json_str):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO recurring_schedules(user_id, data)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET data=excluded.data
    ''', (user_id, data_json_str))
    db.commit()

def load_recurring_schedule_from_db(user_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT data FROM recurring_schedules WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    return row['data'] if row else None

# ----------------------
# Flask-Login user model
# ----------------------
class User(UserMixin):
    def __init__(self, id_, username, password_hash):
        self.id = id_
        self.username = username
        self.password_hash = password_hash

    @staticmethod
    def get(user_id):
        db = get_db()
        cursor = db.cursor()
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        if row:
            return User(row['id'], row['username'], row['password_hash'])
        return None

    @staticmethod
    def find_by_username(username):
        db = get_db()
        cursor = db.cursor()
        cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
        row = cursor.fetchone()
        if row:
            return User(row['id'], row['username'], row['password_hash'])
        return None

    @staticmethod
    def create(username, password):
        db = get_db()
        cursor = db.cursor()
        password_hash = generate_password_hash(password)
        try:
            cursor.execute('INSERT INTO users(username, password_hash) VALUES (?, ?)', (username, password_hash))
            db.commit()
            user_id = cursor.lastrowid
            return User(user_id, username, password_hash)
        except sqlite3.IntegrityError:
            raise ValueError("Username already exists")

@login_manager.user_loader
def load_user(user_id):
    try:
        return User.get(int(user_id))
    except:
        return None
# ----------------------
# Add appointment routes
# ----------------------
@app.route('/add_specific_appointment', methods=['POST'])
@login_required
def add_specific_appointment():
    """
    Handle manual addition of a specific-date appointment via form.
    Expects form fields:
      agency_number, account_name, area,
      min_weight, max_weight,
      start_datetime (input type="datetime-local"),
      end_datetime   (input type="datetime-local")
    """
    user_id = current_user.id

    if request.is_json:
        data = request.get_json(silent=True) or {}
        agency_number = str(data.get('agency_number', '')).strip()
        account_name  = str(data.get('account_name', '')).strip()
        area          = str(data.get('area', '')).strip()
        min_w_raw     = str(data.get('min_weight', '')).strip()
        max_w_raw     = str(data.get('max_weight', '')).strip()
        start_dt_raw  = str(data.get('start_time', '')).strip()
        end_dt_raw    = str(data.get('end_time', '')).strip()
    else:
        agency_number = request.form.get('agency_number', '').strip()
        account_name  = request.form.get('account_name', '').strip()
        area          = request.form.get('area', '').strip()
        min_w_raw     = request.form.get('min_weight', '').strip()
        max_w_raw     = request.form.get('max_weight', '').strip()
        start_dt_raw  = request.form.get('start_datetime', '').strip()
        end_dt_raw    = request.form.get('end_datetime', '').strip()
    # Basic validation
    errors = []
    if not agency_number:
        errors.append("Agency Number is required.")
    if not account_name:
        errors.append("Account Name is required.")
    if not area:
        errors.append("Area is required.")
    # Validate numeric weights (allow blank or zero if desired)
    try:
        min_w = float(min_w_raw) if min_w_raw else 0.0
    except:
        errors.append("Minimum Weight must be a number.")
    try:
        max_w = float(max_w_raw) if max_w_raw else 0.0
    except:
        errors.append("Maximum Weight must be a number.")
    # Parse datetime-local: format "YYYY-MM-DDTHH:MM"
    try:
        if not start_dt_raw:
            raise ValueError
        # datetime.fromisoformat can parse "YYYY-MM-DDTHH:MM"
        start_dt = datetime.fromisoformat(start_dt_raw)
    except:
        errors.append("Start Time is required and must be in correct format.")
    try:
        if not end_dt_raw:
            raise ValueError
        end_dt = datetime.fromisoformat(end_dt_raw)
    except:
        errors.append("End Time is required and must be in correct format.")
    # If parsed, check end > start
    if 'start_dt' in locals() and 'end_dt' in locals():
        if end_dt <= start_dt:
            errors.append("End Time must be after Start Time.")

    if errors:
        if request.is_json:
            return jsonify({'error': '; '.join(errors)}), 400
        for e in errors:
            flash(e, "error")
        # Redirect back to schedule; user will see flashes
        return redirect(url_for('schedule'))


    # Build new appointment dict matching your existing structure
    start_iso = start_dt.isoformat()
    end_iso   = end_dt.isoformat()
    start_hour = start_dt.hour + start_dt.minute/60.0

    new_appt = {
        'id': str(uuid.uuid4()),
        'agency_number': agency_number,
        'account_name': account_name,
        'area': area,
        'min_weight': min_w,
        'max_weight': max_w,
        'start_time': start_iso,
        'end_time': end_iso,
        'start_hour': start_hour
    }

    # Load existing definitions JSON from DB
    defs_json = load_specific_definitions_from_db(user_id)
    if defs_json:
        try:
            appts = json.loads(defs_json)
        except:
            appts = []
    else:
        appts = []
    # Append
    appts.append(new_appt)
    # Save back to DB
    try:
        save_specific_definitions_to_db(user_id, json.dumps(appts))
    except Exception:
        app.logger.exception("Failed to save new specific appointment")
        if request.is_json:
            return jsonify({'error': 'Failed to persist new appointment'}), 500
        flash("Failed to persist new appointment.", "error")
        # but continue to update in-memory so session sees it
    # Also update in-memory store if present
    if user_id in data_store and isinstance(data_store[user_id], dict):
        # update the list
        data_store[user_id].setdefault('appointments', []).append(new_appt)
    if request.is_json:
        return jsonify({'success': True, 'new_appt': new_appt})
    flash("Added new appointment.", "info")
    # Redirect back to scheduler
    return redirect(url_for('schedule'))


@app.route('/add_recurring_appointment', methods=['POST'])
@login_required
def add_recurring_appointment():
    """
    Handle manual addition of a recurring appointment via form.
    Expects form fields:
      agency_number, account_name, area,
      min_weight, max_weight,
      day (e.g. 'Monday'), frequency (e.g. 'First'),
      start_time_str (input type="time"), end_time_str (input type="time")
    """
    user_id = current_user.id

    if request.is_json:
        data = request.get_json(silent=True) or {}
        agency_number = str(data.get('agency_number', '')).strip()
        account_name  = str(data.get('account_name', '')).strip()
        area          = str(data.get('area', '')).strip()
        min_w_raw     = str(data.get('min_weight', '')).strip()
        max_w_raw     = str(data.get('max_weight', '')).strip()
        day           = str(data.get('day', '')).strip()
        frequency     = str(data.get('frequency', '')).strip()
        start_time_raw= str(data.get('start_time_str', '')).strip()
        end_time_raw  = str(data.get('end_time_str', '')).strip()
    else:
        agency_number = request.form.get('agency_number', '').strip()
        account_name  = request.form.get('account_name', '').strip()
        area          = request.form.get('area', '').strip()
        min_w_raw     = request.form.get('min_weight', '').strip()
        max_w_raw     = request.form.get('max_weight', '').strip()
        day           = request.form.get('day', '').strip()
        frequency     = request.form.get('frequency', '').strip()
        start_time_raw= request.form.get('start_time', '').strip()  # format "HH:MM"
        end_time_raw  = request.form.get('end_time', '').strip()

    # Validate
    weekdays = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals = ['First','Second','Third','Fourth','Fifth']
    errors = []
    if not agency_number:
        errors.append("Agency Number is required.")
    if not account_name:
        errors.append("Account Name is required.")
    if not area:
        errors.append("Area is required.")
    try:
        min_w = float(min_w_raw) if min_w_raw else 0.0
    except:
        errors.append("Minimum Weight must be a number.")
    try:
        max_w = float(max_w_raw) if max_w_raw else 0.0
    except:
        errors.append("Maximum Weight must be a number.")
    if day not in weekdays:
        errors.append(f"Day must be one of {weekdays}.")
    if frequency not in ordinals:
        errors.append(f"Frequency must be one of {ordinals}.")
    # Parse time strings
    try:
        if not start_time_raw:
            raise ValueError
        # parse "HH:MM" into a datetime.time
        from datetime import time
        h,m = map(int, start_time_raw.split(':'))
        start_hour = h + m/60.0
    except:
        errors.append("Start Time is required and must be HH:MM.")
    try:
        if not end_time_raw:
            raise ValueError
        h2,m2 = map(int, end_time_raw.split(':'))
        end_hour = h2 + m2/60.0
    except:
        errors.append("End Time is required and must be HH:MM.")
    if 'start_hour' in locals() and 'end_hour' in locals():
        if end_hour <= start_hour:
            errors.append("End Time must be after Start Time.")

    if errors:
        if request.is_json:
            return jsonify({'error': '; '.join(errors)}), 400
        for e in errors:
            flash(e, "error")
        return redirect(url_for('recurring_schedule'))

    # Build new recurring appointment dict matching your structure:
    new_appt = {
        'id': str(uuid.uuid4()),
        'agency_number': agency_number,
        'account_name': account_name,
        'area': area,
        'min_weight': min_w,
        'max_weight': max_w,
        'day': day,
        'frequency': frequency,
        'start_hour': start_hour,
        'end_hour': end_hour,
        'start_time_str': start_time_raw,
        'end_time_str': end_time_raw
    }

    # Load existing recurring definitions JSON
    defs_json = load_recurring_definitions_from_db(user_id)
    if defs_json:
        try:
            appts = json.loads(defs_json)
        except:
            appts = []
    else:
        appts = []
    appts.append(new_appt)
    # Save back
    try:
        save_recurring_definitions_to_db(user_id, json.dumps(appts))
    except Exception:
        app.logger.exception("Failed to save new recurring appointment")
        if request.is_json:
            return jsonify({'error': 'Failed to persist new appointment'}), 500
        flash("Failed to persist new recurring appointment.", "error")
    # Update in-memory
    # Recompute patterns too
    from datetime import datetime as _dt  # if needed
    weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals_order = ['First','Second','Third','Fourth','Fifth']
    patterns = []
    seen = set()
    for a in appts:
        label = f"{a.get('frequency')} {a.get('day')}"
        if label not in seen:
            seen.add(label)
            patterns.append({'label': label, 'frequency': a.get('frequency'), 'day': a.get('day')})
    patterns.sort(key=lambda p: (ordinals_order.index(p['frequency']), weekdays_order.index(p['day'])))

    if user_id in data_store:
        data_store[user_id]['recurring'] = {
            'appointments': appts,
            'patterns': patterns
        }
    if request.is_json:
        return jsonify({'success': True, 'new_appt': new_appt})
    flash("Added new recurring appointment.", "info")
    return redirect(url_for('recurring_schedule'))
# ----------------------
# Edit/delete routes
# ----------------------
@app.route('/edit_specific_appointment', methods=['POST'])
@login_required
def edit_specific_appointment():
    """
    Edit a specific-date appointment definition.
    Expects JSON:
    {
      "id": "<appointment-id>",
      "agency_number": "...",
      "account_name": "...",
      "area": "...",
      "min_weight": <number>,
      "max_weight": <number>,
      "start_time": "YYYY-MM-DDTHH:MM:SS",
      "end_time":   "YYYY-MM-DDTHH:MM:SS"
    }
    """
    user_id = current_user.id
    data = request.get_json()
    if not isinstance(data, dict) or 'id' not in data:
        return jsonify({'error': 'Invalid payload'}), 400
    appt_id = data['id']
    # Load definitions from DB
    defs_json = load_specific_definitions_from_db(user_id)
    if not defs_json:
        return jsonify({'error': 'No definitions found'}), 400
    try:
        appointments = json.loads(defs_json)
    except:
        return jsonify({'error': 'Failed to parse definitions'}), 500

    # Find and update
    updated_appt = None
    for appt in appointments:
        if appt.get('id') == appt_id:
            # Validate required fields
            try:
                agency_number = str(data.get('agency_number','')).strip()
                account_name = str(data.get('account_name','')).strip()
                area = str(data.get('area','')).strip()
                min_w = data.get('min_weight')
                max_w = data.get('max_weight')
                start_time = data.get('start_time')
                end_time   = data.get('end_time')
                if not agency_number or not account_name or not area \
                   or min_w is None or max_w is None \
                   or not start_time or not end_time:
                    return jsonify({'error': 'All fields required'}), 400
                st_dt = pd.to_datetime(start_time)
                et_dt = pd.to_datetime(end_time)
                if et_dt <= st_dt:
                    return jsonify({'error': 'End Time must be after Start Time'}), 400
                start_iso = st_dt.isoformat()
                end_iso   = et_dt.isoformat()
                start_hour = st_dt.hour + st_dt.minute/60.0
            except Exception as e:
                return jsonify({'error': f'Invalid data: {e}'}), 400
            # Apply updates
            appt.update({
                'agency_number': agency_number,
                'account_name': account_name,
                'area': area,
                'min_weight': min_w,
                'max_weight': max_w,
                'start_time': start_iso,
                'end_time': end_iso,
                'start_hour': start_hour
            })
            updated_appt = appt
            break
    if updated_appt is None:
        return jsonify({'error': 'Appointment ID not found'}), 404

    # Save back to DB
    try:
        save_specific_definitions_to_db(user_id, json.dumps(appointments))
    except:
        return jsonify({'error': 'Failed to save changes'}), 500

    # Update in-memory store if loaded
    store = data_store.get(user_id)
    if store and 'appointments' in store:
        for i,a in enumerate(store['appointments']):
            if a.get('id') == appt_id:
                store['appointments'][i] = updated_appt
                break

    return jsonify({'success': True, 'updated_appt': updated_appt})


@app.route('/delete_specific_appointment', methods=['POST'])
@login_required
def delete_specific_appointment():
    """
    Delete a specific-date appointment definition by ID.
    Expects JSON { "id": "<appointment-id>" }.
    """
    user_id = current_user.id
    data = request.get_json()
    if not isinstance(data, dict) or 'id' not in data:
        return jsonify({'error': 'Invalid payload'}), 400
    appt_id = data['id']
    defs_json = load_specific_definitions_from_db(user_id)
    if not defs_json:
        return jsonify({'error': 'No definitions found'}), 400
    try:
        appointments = json.loads(defs_json)
    except:
        return jsonify({'error': 'Failed to parse definitions'}), 500

    new_apps = [a for a in appointments if a.get('id') != appt_id]
    if len(new_apps) == len(appointments):
        return jsonify({'error': 'Appointment ID not found'}), 404

    # Save updated definitions
    try:
        save_specific_definitions_to_db(user_id, json.dumps(new_apps))
    except:
        return jsonify({'error': 'Failed to delete appointment'}), 500

    # Update in-memory
    store = data_store.get(user_id)
    if store and 'appointments' in store:
        store['appointments'] = [a for a in store['appointments'] if a.get('id') != appt_id]

    # Clean up any saved schedule assignments containing this ID
    saved = load_specific_schedule_from_db(user_id)
    if saved:
        try:
            assignments = json.loads(saved)
            modified = False
            for date_str, slots in assignments.items():
                for slotKey, id_list in slots.items():
                    if appt_id in id_list:
                        id_list[:] = [i for i in id_list if i != appt_id]
                        modified = True
            if modified:
                save_specific_schedule_to_db(user_id, json.dumps(assignments))
        except:
            pass

    return jsonify({'success': True})


@app.route('/edit_recurring_appointment', methods=['POST'])
@login_required
def edit_recurring_appointment():
    """
    Edit a recurring appointment definition.
    Expects JSON:
    {
      "id": "<appointment-id>",
      "agency_number": "...",
      "account_name": "...",
      "area": "...",
      "min_weight": <number>,
      "max_weight": <number>,
      "day": "Monday"/...,
      "frequency": "First"/...,
      "start_time_str": "HH:MM",
      "end_time_str": "HH:MM"
    }
    """
    user_id = current_user.id
    data = request.get_json()
    if not isinstance(data, dict) or 'id' not in data:
        return jsonify({'error': 'Invalid payload'}), 400
    appt_id = data['id']
    defs_json = load_recurring_definitions_from_db(user_id)
    if not defs_json:
        return jsonify({'error': 'No definitions found'}), 400
    try:
        appointments_rec = json.loads(defs_json)
    except:
        return jsonify({'error': 'Failed to parse definitions'}), 500

    weekdays = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals = ['First','Second','Third','Fourth','Fifth']
    updated_appt = None
    for appt in appointments_rec:
        if appt.get('id') == appt_id:
            try:
                agency_number = str(data.get('agency_number','')).strip()
                account_name  = str(data.get('account_name','')).strip()
                area          = str(data.get('area','')).strip()
                min_w         = data.get('min_weight')
                max_w         = data.get('max_weight')
                day           = str(data.get('day','')).strip()
                frequency     = str(data.get('frequency','')).strip()
                st_str        = data.get('start_time_str','')
                et_str        = data.get('end_time_str','')
                if not agency_number or not account_name or not area \
                   or min_w is None or max_w is None \
                   or day not in weekdays or frequency not in ordinals \
                   or not st_str or not et_str:
                    return jsonify({'error': 'All fields required and valid'}), 400
                st_dt = pd.to_datetime(st_str, format='%H:%M')
                et_dt = pd.to_datetime(et_str, format='%H:%M')
                start_hour = st_dt.hour + st_dt.minute/60.0
                end_hour   = et_dt.hour + et_dt.minute/60.0
                if not (end_hour > start_hour):
                    return jsonify({'error': 'End Time must be after Start Time'}), 400
            except Exception as e:
                return jsonify({'error': f'Invalid data: {e}'}), 400
            # Update fields
            appt.update({
                'agency_number': agency_number,
                'account_name': account_name,
                'area': area,
                'min_weight': min_w,
                'max_weight': max_w,
                'day': day,
                'frequency': frequency,
                'start_time_str': st_str,
                'end_time_str': et_str,
                'start_hour': start_hour,
                'end_hour': end_hour
            })
            updated_appt = appt
            break
    if updated_appt is None:
        return jsonify({'error': 'Appointment ID not found'}), 404

    # Save back
    try:
        save_recurring_definitions_to_db(user_id, json.dumps(appointments_rec))
    except:
        return jsonify({'error': 'Failed to save changes'}), 500

    # Update in-memory
    store = data_store.get(user_id)
    if store and 'recurring' in store:
        apps = store['recurring']['appointments']
        for i,a in enumerate(apps):
            if a.get('id') == appt_id:
                apps[i] = updated_appt
                break
        # Recompute patterns if day/frequency changed:
        weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        ordinals_order = ['First','Second','Third','Fourth','Fifth']
        patterns_new = []
        seen = set()
        for a in store['recurring']['appointments']:
            label = f"{a['frequency']} {a['day']}"
            if label not in seen:
                seen.add(label)
                patterns_new.append({'label': label, 'frequency': a['frequency'], 'day': a['day']})
        patterns_new.sort(key=lambda p: (ordinals_order.index(p['frequency']), weekdays_order.index(p['day'])))
        store['recurring']['patterns'] = patterns_new

    return jsonify({'success': True, 'updated_appt': updated_appt})


@app.route('/delete_recurring_appointment', methods=['POST'])
@login_required
def delete_recurring_appointment():
    """
    Delete a recurring appointment definition by ID.
    Expects JSON { "id": "<appointment-id>" }.
    """
    user_id = current_user.id
    data = request.get_json()
    if not isinstance(data, dict) or 'id' not in data:
        return jsonify({'error': 'Invalid payload'}), 400
    appt_id = data['id']
    defs_json = load_recurring_definitions_from_db(user_id)
    if not defs_json:
        return jsonify({'error': 'No definitions found'}), 400
    try:
        appointments_rec = json.loads(defs_json)
    except:
        return jsonify({'error': 'Failed to parse definitions'}), 500

    new_apps = [a for a in appointments_rec if a.get('id') != appt_id]
    if len(new_apps) == len(appointments_rec):
        return jsonify({'error': 'Appointment ID not found'}), 404

    try:
        save_recurring_definitions_to_db(user_id, json.dumps(new_apps))
    except:
        return jsonify({'error': 'Failed to delete appointment'}), 500

    # Update in-memory
    store = data_store.get(user_id)
    if store and 'recurring' in store:
        store['recurring']['appointments'] = new_apps
        # Recompute patterns
        weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        ordinals_order = ['First','Second','Third','Fourth','Fifth']
        patterns_new = []
        seen = set()
        for a in new_apps:
            label = f"{a['frequency']} {a['day']}"
            if label not in seen:
                seen.add(label)
                patterns_new.append({'label': label, 'frequency': a['frequency'], 'day': a['day']})
        patterns_new.sort(key=lambda p: (ordinals_order.index(p['frequency']), weekdays_order.index(p['day'])))
        store['recurring']['patterns'] = patterns_new

    # Clean up saved assignments
    saved = load_recurring_schedule_from_db(user_id)
    if saved:
        try:
            assignments = json.loads(saved)
            modified = False
            for patLabel, slots in assignments.items():
                for slotKey, id_list in slots.items():
                    if appt_id in id_list:
                        id_list[:] = [i for i in id_list if i != appt_id]
                        modified = True
            if modified:
                save_recurring_schedule_to_db(user_id, json.dumps(assignments))
        except:
            pass

    return jsonify({'success': True})
# ----------------------
# Authentication routes
# ----------------------
@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        password2 = request.form.get('password2', '')
        if not username or not password or not password2:
            flash("Please fill all fields", "error")
            return redirect(request.url)
        if password != password2:
            flash("Passwords do not match", "error")
            return redirect(request.url)
        try:
            user = User.create(username, password)
        except ValueError as e:
            flash(str(e), "error")
            return redirect(request.url)
        flash("Registration successful. Please log in.", "info")
        return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        if not username or not password:
            flash("Please fill both username and password", "error")
            return redirect(request.url)
        user = User.find_by_username(username)
        if user and check_password_hash(user.password_hash, password):
            login_user(user)
            flash("Logged in successfully", "info")
            next_page = request.args.get('next')
            # TODO: validate next_page is safe
            return redirect(next_page or url_for('index'))
        else:
            flash("Invalid username or password", "error")
            return redirect(request.url)
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash("You have been logged out.", "info")
    return redirect(url_for('login'))

# ----------------------
# Home page after login
# ----------------------
@app.route('/', methods=['GET'])
@login_required
def index():
    """
    Home page: let user choose between specific-date or recurring upload.
    """
    return render_template('index.html')

# ----------------------
# Specific-date scheduling flow
# ----------------------
@app.route('/upload_specific', methods=['GET', 'POST'])
@login_required
def upload_specific():
    """
    Upload a CSV for specific-date appointments, or use existing definitions if any.
    Expects columns: Agency Number, Account Name, Area, Minimum Weight, Maximum Weight, Start Time, End Time
    """
    user_id = current_user.id
    if request.method == 'POST':
        # Detect "Use existing upload" button
        if request.form.get('use_existing'):
            return redirect(url_for('use_existing_specific'))

        # Otherwise, process uploaded file
        if 'file' not in request.files:
            flash("No file part", "error")
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash("No selected file", "error")
            return redirect(request.url)
        try:
            df = pd.read_csv(file, parse_dates=['Start Time', 'End Time'])
        except Exception as e:
            flash(f"Failed to read CSV: {e}", "error")
            return redirect(request.url)
        required = [
            'Agency Number',
            'Account Name',
            'Area',
            'Minimum Weight',
            'Maximum Weight',
            'Start Time',
            'End Time'
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            flash(f"Missing required columns: {missing}", "error")
            return redirect(request.url)
        appointments = []
        for idx, row in df.iterrows():
            st = row['Start Time']
            et = row['End Time']
            if not pd.notnull(st) or not pd.notnull(et):
                continue
            try:
                start_iso = pd.to_datetime(st).isoformat()
                end_iso = pd.to_datetime(et).isoformat()
                start_dt = pd.to_datetime(st)
                start_hour = start_dt.hour + start_dt.minute/60.0
            except:
                continue
            appointments.append({
                'id': str(uuid.uuid4()),
                'agency_number': str(row['Agency Number']),
                'account_name': str(row['Account Name']),
                'area': str(row['Area']),
                'min_weight': row['Minimum Weight'],
                'max_weight': row['Maximum Weight'],
                'start_time': start_iso,
                'end_time': end_iso,
                'start_hour': start_hour
            })
        if not appointments:
            flash("No valid appointments found in CSV.", "error")
            return redirect(request.url)
        # Persist definitions
        defs_json = json.dumps(appointments)
        save_specific_definitions_to_db(user_id, defs_json)
        # Load into in-memory store
        data_store[user_id] = {
            'appointments': appointments,
            'config': {}
        }
        # Optionally clear previous saved schedule:
        # save_specific_schedule_to_db(user_id, "{}")
        flash("Upload successful; definitions saved. Now choose date range.", "info")
        return redirect(url_for('configure'))
    # GET
    existing_defs = load_specific_definitions_from_db(user_id)
    return render_template('upload_specific.html', existing_defs=bool(existing_defs))

@app.route('/use_existing_specific', methods=['POST'])
@login_required
def use_existing_specific():
    """
    Load previously uploaded specific-date definitions into memory and go to configure.
    """
    user_id = current_user.id
    defs_json = load_specific_definitions_from_db(user_id)
    if not defs_json:
        flash("No existing upload found. Please upload a file.", "error")
        return redirect(url_for('upload_specific'))
    try:
        appointments = json.loads(defs_json)
    except:
        flash("Failed to load existing definitions; please upload again.", "error")
        return redirect(url_for('upload_specific'))
    data_store[user_id] = {
        'appointments': appointments,
        'config': {}
    }
    flash("Loaded existing recurring definitions. You can continue assigning.", "info")
    return redirect(url_for('recurring_schedule'))

    if not new_defs:
        flash("No valid recurring definitions found in CSV.", "error")
        return redirect(url_for('recurring_upload'))

    existing_json = load_recurring_definitions_from_db(user_id)
    if existing_json:
        try:
            existing_defs = json.loads(existing_json)
        except Exception:
            existing_defs = []
    else:
        existing_defs = []

    replaced_ids = set()
    for new_rec in new_defs:
        match_idx = -1
        for i, rec in enumerate(existing_defs):
            if (rec.get('agency_number') == new_rec['agency_number'] and
                rec.get('account_name') == new_rec['account_name'] and
                rec.get('day') == new_rec['day'] and
                rec.get('frequency') == new_rec['frequency']):
                match_idx = i
                break
        if match_idx == -1:
            existing_defs.append(new_rec)
            continue
        old = existing_defs[match_idx]
        equal_all = all(old.get(k) == new_rec.get(k) for k in new_rec if k != 'id')
        if equal_all:
            continue
        replaced_ids.add(old.get('id'))
        existing_defs[match_idx] = new_rec

    if replaced_ids:
        sched_json = load_recurring_schedule_from_db(user_id)
        if sched_json:
            try:
                assignments = json.loads(sched_json)
            except Exception:
                assignments = {}
            modified = False
            for pat, slots in assignments.items():
                if not isinstance(slots, dict):
                    continue
                for slot_key, id_list in slots.items():
                    if not isinstance(id_list, list):
                        continue
                    new_list = [i for i in id_list if i not in replaced_ids]
                    if len(new_list) != len(id_list):
                        assignments[pat][slot_key] = new_list
                        modified = True
            if modified:
                save_recurring_schedule_to_db(user_id, json.dumps(assignments))

    save_recurring_definitions_to_db(user_id, json.dumps(existing_defs))

    weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    ordinals_order = ['First', 'Second', 'Third', 'Fourth', 'Fifth']
    patterns = []
    seen = set()
    for appt in existing_defs:
        label = f"{appt['frequency']} {appt['day']}"
        if label not in seen:
            seen.add(label)
            patterns.append({'label': label, 'frequency': appt['frequency'], 'day': appt['day']})
    patterns.sort(key=lambda p: (ordinals_order.index(p['frequency']), weekdays_order.index(p['day'])))

    data_store[user_id] = {
        'recurring': {
            'appointments': existing_defs,
            'patterns': patterns
        }
    }

    flash("Recurring definitions merged successfully.", "info")
    return redirect(url_for('recurring_schedule'))

@app.route('/configure', methods=['GET', 'POST'])
@login_required
def configure():
    """
    Let user specify date range for specific-date scheduling.
    """
    user_id = current_user.id
    store = data_store.get(user_id)
    if store is None or 'appointments' not in store:
        flash("No uploaded appointments. Please upload CSV first.", "error")
        return redirect(url_for('upload_specific'))

    if request.method == 'POST':
        date_start = request.form.get('date_start')
        date_end = request.form.get('date_end')
        if not date_start or not date_end:
            flash("Please fill both start date and end date.", "error")
            return redirect(request.url)
        try:
            dt_start = datetime.fromisoformat(date_start).date()
            dt_end = datetime.fromisoformat(date_end).date()
        except Exception as e:
            flash(f"Invalid date format: {e}", "error")
            return redirect(request.url)
        if dt_end < dt_start:
            flash("End date must be on or after start date.", "error")
            return redirect(request.url)
        # Generate list of ISO date strings
        dates = []
        cur = dt_start
        while cur <= dt_end:
            dates.append(cur.isoformat())
            cur = cur + timedelta(days=1)
        store['config'] = {
            'date_start': dt_start.isoformat(),
            'date_end': dt_end.isoformat(),
            'dates': dates
        }
        data_store[user_id] = store
        return redirect(url_for('schedule'))
    # GET
    return render_template('configure.html')

@app.route('/schedule', methods=['GET'])
@login_required
def schedule():
    """
    Display drag-and-drop UI for specific-date appointments.
    Auto-load definitions if in-memory missing but exist in DB.
    """
    user_id = current_user.id
    store = data_store.get(user_id)

    # Auto-load definitions if missing
    if (store is None or 'appointments' not in store) and load_specific_definitions_from_db(user_id):
        try:
            defs_json = load_specific_definitions_from_db(user_id)
            appointments = json.loads(defs_json)
        except:
            appointments = []
        data_store[user_id] = {'appointments': appointments, 'config': {}}
        flash("Loaded your previously uploaded appointments. Please choose date range.", "info")
        return redirect(url_for('configure'))

    # Now require both appointments and config
    store = data_store.get(user_id)
    if store is None or 'appointments' not in store or 'config' not in store or not store['config']:
        flash("Missing configuration. Please upload or load definitions first.", "error")
        return redirect(url_for('upload_specific'))

    appointments = store['appointments']
    cfg = store['config']
    dates = cfg.get('dates', [])

    # Load saved assignments if any
    saved_json_str = load_specific_schedule_from_db(user_id)
    saved_assignments = None
    if saved_json_str:
        try:
            saved_assignments = json.loads(saved_json_str)
        except:
            saved_assignments = None

    return render_template(
        'schedule.html',
        appointments=appointments,
        dates=dates,
        trucks=FIXED_TRUCKS,
        slots=SLOT_DEFINITIONS,
        saved_assignments=saved_assignments
    )

@app.route('/save_schedule', methods=['POST'])
@login_required
def save_schedule():
    """
    Persist specific-date schedule assignments for this user.
    Expects JSON:
    {
      "YYYY-MM-DD": {
         "Trailer 1_A": [id1, id2, ...],
         ...
      },
      ...
    }
    Returns JSON { "success": True } (or error).
    """
    user_id = current_user.id
    store = data_store.get(user_id)
    # We may not need in-memory store[`config`] here, since we persist by saved JSON.
    # But we still require definitions exist:
    # Attempt to load definitions if not in memory:
    if (store is None or 'appointments' not in store) and load_specific_definitions_from_db(user_id):
        try:
            defs_json = load_specific_definitions_from_db(user_id)
            appointments = json.loads(defs_json)
        except:
            appointments = []
        store = {'appointments': appointments, 'config': {}}
        data_store[user_id] = store

    if store is None or 'appointments' not in store:
        return jsonify({'error': 'No appointment definitions found'}), 400

    data = request.get_json()
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid JSON'}), 400

    # Persist assignments JSON
    try:
        json_str = json.dumps(data)
        save_specific_schedule_to_db(user_id, json_str)
        app.logger.info(f"Saved specific schedule for user {user_id}")
    except Exception:
        app.logger.exception("Failed to save specific schedule to DB")
        return jsonify({'error': 'Failed to save schedule'}), 500

    return jsonify({'success': True})

@app.route('/download_schedule', methods=['GET'])
@login_required
def download_schedule():
    """
    Generate and return CSV for the user’s saved specific-date schedule assignments.
    Reads saved assignments from DB, loads definitions, constructs CSV rows.
    """
    user_id = current_user.id

    # Load definitions from DB
    defs_json = load_specific_definitions_from_db(user_id)
    if not defs_json:
        return flash("No appointment definitions found. Please upload first.", "error") or redirect(url_for('upload_specific'))

    try:
        appointments = json.loads(defs_json)
    except:
        flash("Failed to load appointment definitions.", "error")
        return redirect(url_for('upload_specific'))

    # Load saved assignments JSON
    saved_json_str = load_specific_schedule_from_db(user_id)
    if not saved_json_str:
        flash("No saved schedule found. Please arrange and click Save first.", "error")
        return redirect(url_for('schedule'))

    try:
        assignments = json.loads(saved_json_str)
    except:
        flash("Failed to load saved schedule.", "error")
        return redirect(url_for('schedule'))

    # Build a lookup from appointment id to data
    appt_lookup = {appt['id']: appt for appt in appointments}

    out_rows = []
    for date_str, slots_dict in assignments.items():
        if not isinstance(slots_dict, dict):
            continue
        for slot_key, appt_ids in slots_dict.items():
            if not isinstance(appt_ids, list):
                continue
            parts = slot_key.rsplit('_', 1)
            if len(parts) == 2:
                truck_name, slot_label = parts
            else:
                truck_name = slot_key
                slot_label = ""
            for aid in appt_ids:
                appt = appt_lookup.get(aid)
                if not appt:
                    continue
                out_rows.append({
                    'Date': date_str,
                    'Truck Name': truck_name,
                    'Slot': slot_label,
                    'Agency Number': appt['agency_number'],
                    'Account Name': appt['account_name'],
                    'Area': appt['area'],
                    'Minimum Weight': appt['min_weight'],
                    'Maximum Weight': appt['max_weight'],
                    'Start Time': appt['start_time'],
                    'End Time': appt['end_time']
                })
    if not out_rows:
        flash("No appointments scheduled to download.", "error")
        return redirect(url_for('schedule'))

    df_out = pd.DataFrame(out_rows)
    cols = ['Date','Truck Name','Slot','Agency Number','Account Name','Area','Minimum Weight','Maximum Weight','Start Time','End Time']
    df_out = df_out[cols]
    csv_text = df_out.to_csv(index=False)
    buf = BytesIO(csv_text.encode('utf-8'))
    buf.seek(0)
    return send_file(
        buf,
        mimetype='text/csv',
        as_attachment=True,
        download_name='scheduled_routes.csv'
    )


# ----------------------
# Recurring scheduling flow
# ----------------------
@app.route('/recurring_upload', methods=['GET', 'POST'])
@login_required
def recurring_upload():
    """
    Upload CSV defining recurring appointments, or use existing definitions.
    Expects columns: Agency Number, Account Name, Area, Minimum Weight, Maximum Weight,
                     Day, Frequency, Start Time, End Time
    """
    user_id = current_user.id
    if request.method == 'POST':
        # Detect "Use existing upload" button
        if request.form.get('use_existing'):
            return redirect(url_for('use_existing_recurring'))

        # Otherwise, process file upload
        if 'file' not in request.files:
            flash("No file part", "error")
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash("No selected file", "error")
            return redirect(request.url)
        try:
            df = pd.read_csv(file)
        except Exception as e:
            flash(f"Failed to read CSV: {e}", "error")
            return redirect(request.url)
        required = [
            'Agency Number', 'Account Name', 'Area',
            'Minimum Weight', 'Maximum Weight',
            'Day', 'Frequency', 'Start Time', 'End Time'
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            flash(f"Missing required columns: {missing}", "error")
            return redirect(request.url)
        appointments_rec = []
        weekdays = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        ordinals = ['First','Second','Third','Fourth','Fifth']
        for idx, row in df.iterrows():
            try:
                agency_number = str(row['Agency Number'])
                account_name = str(row['Account Name'])
                area = str(row['Area'])
                min_w = row['Minimum Weight']
                max_w = row['Maximum Weight']
                day = str(row['Day']).strip()
                frequency = str(row['Frequency']).strip()
                st_raw = row['Start Time']
                et_raw = row['End Time']
            except Exception:
                continue
            if day not in weekdays:
                flash(f"Row {idx+2}: Invalid Day '{day}'. Must be one of {weekdays}.", "error")
                return redirect(request.url)
            if frequency not in ordinals:
                flash(f"Row {idx+2}: Invalid Frequency '{frequency}'. Must be one of {ordinals}.", "error")
                return redirect(request.url)
            # Parse times
            try:
                st_dt = pd.to_datetime(st_raw)
                et_dt = pd.to_datetime(et_raw)
                start_hour = st_dt.hour + st_dt.minute/60.0
                end_hour = et_dt.hour + et_dt.minute/60.0
                start_time_str = st_dt.strftime('%H:%M')
                end_time_str = et_dt.strftime('%H:%M')
            except Exception as e:
                flash(f"Row {idx+2}: Cannot parse Start/End Time '{st_raw}'/'{et_raw}': {e}", "error")
                return redirect(request.url)
            if not (end_hour > start_hour):
                flash(f"Row {idx+2}: End Time must be after Start Time.", "error")
                return redirect(request.url)
            appointments_rec.append({
                'id': str(uuid.uuid4()),
                'agency_number': agency_number,
                'account_name': account_name,
                'area': area,
                'min_weight': min_w,
                'max_weight': max_w,
                'day': day,
                'frequency': frequency,
                'start_hour': start_hour,
                'end_hour': end_hour,
                'start_time_str': start_time_str,
                'end_time_str': end_time_str
            })
        if not appointments_rec:
            flash("No valid recurring definitions found in CSV.", "error")
            return redirect(request.url)
        # Persist definitions
        defs_json = json.dumps(appointments_rec)
        save_recurring_definitions_to_db(user_id, defs_json)
        # Compute patterns
        patterns = []
        seen = set()
        weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        ordinals_order = ['First','Second','Third','Fourth','Fifth']
        for appt in appointments_rec:
            label = f"{appt['frequency']} {appt['day']}"
            if label not in seen:
                seen.add(label)
                patterns.append({'label': label, 'frequency': appt['frequency'], 'day': appt['day']})
        patterns.sort(key=lambda pat: (ordinals_order.index(pat['frequency']), weekdays_order.index(pat['day'])))
        # Store in-memory
        data_store[user_id] = {
            'recurring': {
                'appointments': appointments_rec,
                'patterns': patterns
            }
        }
        # Optionally clear previous recurring schedule:
        # save_recurring_schedule_to_db(user_id, "{}")
        flash("Recurring upload successful; definitions saved. Now assign patterns.", "info")
        return redirect(url_for('recurring_schedule'))
    # GET
    existing_defs = load_recurring_definitions_from_db(user_id)
    return render_template('recurring_upload.html', existing_defs=bool(existing_defs))

@app.route('/use_existing_recurring', methods=['POST'])
@login_required
def use_existing_recurring():
    """
    Load previously uploaded recurring definitions into memory and go to recurring_schedule.
    """
    user_id = current_user.id
    defs_json = load_recurring_definitions_from_db(user_id)
    if not defs_json:
        flash("No existing recurring upload found. Please upload a file.", "error")
        return redirect(url_for('recurring_upload'))
    try:
        appointments_rec = json.loads(defs_json)
    except:
        flash("Failed to load existing definitions; please upload again.", "error")
        return redirect(url_for('recurring_upload'))
    # Compute patterns
    weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals_order = ['First','Second','Third','Fourth','Fifth']
    patterns = []
    seen = set()
    for appt in appointments_rec:
        label = f"{appt.get('frequency')} {appt.get('day')}"
        if label not in seen:
            seen.add(label)
            patterns.append({'label': label, 'frequency': appt.get('frequency'), 'day': appt.get('day')})
    patterns.sort(key=lambda pat: (ordinals_order.index(pat['frequency']), weekdays_order.index(pat['day'])))
    data_store[user_id] = {
        'recurring': {
            'appointments': appointments_rec,
            'patterns': patterns
        }
    }
    flash("Loaded existing recurring definitions. You can continue assigning.", "info")
    return redirect(url_for('recurring_schedule'))

@app.route('/upload_recurring_merge', methods=['POST'])
@login_required
def upload_recurring_merge():
    """Merge uploaded recurring CSV with existing definitions, preserving
    assignments for unchanged appointments. Replaced appointments receive new
    IDs and are removed from any saved assignments."""
    user_id = current_user.id
    if 'file' not in request.files:
        flash("No file part", "error")
        return redirect(url_for('recurring_upload'))
    file = request.files['file']
    if file.filename == '':
        flash("No selected file", "error")
        return redirect(url_for('recurring_upload'))
    try:
        df = pd.read_csv(file)
    except Exception as e:
        flash(f"Failed to read CSV: {e}", "error")
        return redirect(url_for('recurring_upload'))

    required = [
        'Agency Number', 'Account Name', 'Area',
        'Minimum Weight', 'Maximum Weight',
        'Day', 'Frequency', 'Start Time', 'End Time'
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        flash(f"Missing required columns: {missing}", "error")
        return redirect(url_for('recurring_upload'))

    weekdays = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals = ['First','Second','Third','Fourth','Fifth']
    new_rows = []
    for idx, row in df.iterrows():
        try:
            day = str(row['Day']).strip()
            freq = str(row['Frequency']).strip()
            if day not in weekdays or freq not in ordinals:
                raise ValueError
            st_dt = pd.to_datetime(row['Start Time'])
            et_dt = pd.to_datetime(row['End Time'])
            start_hour = st_dt.hour + st_dt.minute/60.0
            end_hour = et_dt.hour + et_dt.minute/60.0
            if not (end_hour > start_hour):
                raise ValueError
            new_rows.append({
                'agency_number': str(row['Agency Number']),
                'account_name': str(row['Account Name']),
                'area': str(row['Area']),
                'min_weight': row['Minimum Weight'],
                'max_weight': row['Maximum Weight'],
                'day': day,
                'frequency': freq,
                'start_hour': start_hour,
                'end_hour': end_hour,
                'start_time_str': st_dt.strftime('%H:%M'),
                'end_time_str': et_dt.strftime('%H:%M')
            })
        except Exception:
            flash(f"Row {idx+2} invalid", "error")
            return redirect(url_for('recurring_upload'))

    # Load existing definitions
    existing_json = load_recurring_definitions_from_db(user_id)
    existing = json.loads(existing_json) if existing_json else []
    def _key(a):
        return (
            a['agency_number'], a['account_name'], a['area'],
            a['day'], a['frequency'], a['start_time_str'], a['end_time_str']
        )
    existing_map = {_key(a): a for a in existing}

    processed_keys = set()
    replaced_ids = []
    merged = []

    for row in new_rows:
        k = _key(row)
        ex = existing_map.get(k)
        if ex:
            processed_keys.add(k)
            identical = (
                ex.get('min_weight') == row['min_weight'] and
                ex.get('max_weight') == row['max_weight']
            )
            if identical:
                row['id'] = ex['id']
            else:
                replaced_ids.append(ex['id'])
                row['id'] = str(uuid.uuid4())
            merged.append(row)
        else:
            row['id'] = str(uuid.uuid4())
            merged.append(row)

    for ex in existing:
        if _key(ex) not in processed_keys:
            merged.append(ex)

    if not merged:
        flash("No valid recurring definitions found in CSV.", "error")
        return redirect(url_for('recurring_upload'))

    try:
        save_recurring_definitions_to_db(user_id, json.dumps(merged))
    except Exception:
        flash("Failed to save merged definitions", "error")
        return redirect(url_for('recurring_upload'))

    # Recompute patterns
    weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals_order = ['First','Second','Third','Fourth','Fifth']
    patterns = []
    seen = set()
    for appt in merged:
        label = f"{appt['frequency']} {appt['day']}"
        if label not in seen:
            seen.add(label)
            patterns.append({'label': label, 'frequency': appt['frequency'], 'day': appt['day']})
    patterns.sort(key=lambda pat: (ordinals_order.index(pat['frequency']), weekdays_order.index(pat['day'])))

    data_store[user_id] = {
        'recurring': {
            'appointments': merged,
            'patterns': patterns
        }
    }

    # Remove replaced IDs from saved assignments
    if replaced_ids:
        saved = load_recurring_schedule_from_db(user_id)
        if saved:
            try:
                assignments = json.loads(saved)
                changed = False
                for pat, slots in assignments.items():
                    for slot, id_list in slots.items():
                        new_list = [i for i in id_list if i not in replaced_ids]
                        if len(new_list) != len(id_list):
                            assignments[pat][slot] = new_list
                            changed = True
                if changed:
                    save_recurring_schedule_to_db(user_id, json.dumps(assignments))
            except Exception:
                pass

    flash("Recurring upload merged; new appointments added to Unassigned.", "info")
    return redirect(url_for('recurring_schedule'))

@app.route('/recurring_schedule', methods=['GET'])
@login_required
def recurring_schedule():
    """
    Display drag-and-drop UI for recurring patterns.
    Auto-load definitions if in-memory missing but exist in DB.
    """
    user_id = current_user.id
    store = data_store.get(user_id)

    # Auto-load if missing
    if (store is None or 'recurring' not in store) and load_recurring_definitions_from_db(user_id):
        try:
            defs_json = load_recurring_definitions_from_db(user_id)
            appointments_rec = json.loads(defs_json)
        except:
            appointments_rec = []
        # Compute patterns
        weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        ordinals_order = ['First','Second','Third','Fourth','Fifth']
        patterns = []
        seen = set()
        for appt in appointments_rec:
            label = f"{appt.get('frequency')} {appt.get('day')}"
            if label not in seen:
                seen.add(label)
                patterns.append({'label': label, 'frequency': appt.get('frequency'), 'day': appt.get('day')})
        patterns.sort(key=lambda pat: (ordinals_order.index(pat['frequency']), weekdays_order.index(pat['day'])))
        data_store[user_id] = {
            'recurring': {
                'appointments': appointments_rec,
                'patterns': patterns
            }
        }
        flash("Loaded your previously uploaded recurring definitions. You can continue assigning.", "info")
    # Now require definitions in memory
    store = data_store.get(user_id)
    if store is None or 'recurring' not in store:
        flash("No recurring definitions found. Please upload first.", "error")
        return redirect(url_for('recurring_upload'))
    recurring = store['recurring']
    appointments_rec = recurring['appointments']
    patterns = recurring['patterns']
    if not patterns:
        flash("No recurrence patterns detected. Ensure CSV has valid Day/Frequency.", "error")
        return redirect(url_for('recurring_upload'))

    # Load saved assignments if any
    saved_json_str = load_recurring_schedule_from_db(user_id)
    saved_assignments = None
    if saved_json_str:
        try:
            saved_assignments = json.loads(saved_json_str)
        except:
            saved_assignments = None

    return render_template(
        'recurring_schedule.html',
        appointments=appointments_rec,
        patterns=patterns,
        trucks=FIXED_TRUCKS,
        slots=SLOT_DEFINITIONS,
        saved_assignments=saved_assignments
    )

@app.route('/save_recurring_schedule', methods=['POST'])
@login_required
def save_recurring_schedule():
    user_id = current_user.id
    store = data_store.get(user_id)
    # (auto-load definitions if needed, similar to specific)
    data = request.get_json()
    if not isinstance(data, dict):
        return jsonify({'error': 'Invalid JSON format'}), 400
    try:
        json_str = json.dumps(data)
        save_recurring_schedule_to_db(user_id, json_str)
    except Exception:
        app.logger.exception("Failed to save recurring schedule to DB")
        return jsonify({'error': 'Failed to save'}), 500
    return jsonify({'success': True})

@app.route('/download_recurring_schedule', methods=['GET'])
@login_required
def download_recurring_schedule():
    user_id = current_user.id
    # Load recurring definitions
    defs_json = load_recurring_definitions_from_db(user_id)
    if not defs_json:
        flash("No recurring definitions found; please upload first.", "error")
        return redirect(url_for('recurring_upload'))
    try:
        appointments_rec = json.loads(defs_json)
    except:
        flash("Failed to load recurring definitions.", "error")
        return redirect(url_for('recurring_upload'))
    # Compute patterns (to know valid labels), or store patterns in DB if previously computed
    weekdays_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    ordinals_order = ['First','Second','Third','Fourth','Fifth']
    patterns = []
    seen = set()
    for appt in appointments_rec:
        label = f"{appt.get('frequency')} {appt.get('day')}"
        if label not in seen:
            seen.add(label)
            patterns.append({'label': label, 'frequency': appt.get('frequency'), 'day': appt.get('day')})
    patterns.sort(key=lambda pat: (ordinals_order.index(pat['frequency']), weekdays_order.index(pat['day'])))
    pattern_map = {p['label']: p for p in patterns}

    # Load saved assignments
    saved_json_str = load_recurring_schedule_from_db(user_id)
    if not saved_json_str:
        flash("No saved recurring schedule found. Please assign and Save first.", "error")
        return redirect(url_for('recurring_schedule'))
    try:
        assignments = json.loads(saved_json_str)
    except:
        flash("Failed to load saved recurring schedule.", "error")
        return redirect(url_for('recurring_schedule'))

    appt_lookup = {appt['id']: appt for appt in appointments_rec}
    out_rows = []
    for pattern_label, slots_dict in assignments.items():
        if pattern_label not in pattern_map:
            continue
        pat = pattern_map[pattern_label]
        freq = pat['frequency']
        day = pat['day']
        if not isinstance(slots_dict, dict):
            continue
        for slot_key, appt_ids in slots_dict.items():
            if not isinstance(appt_ids, list):
                continue
            parts = slot_key.rsplit('_', 1)
            if len(parts) == 2:
                truck_name, slot_label = parts
            else:
                truck_name = slot_key
                slot_label = ""
            for aid in appt_ids:
                appt = appt_lookup.get(aid)
                if not appt:
                    continue
                out_rows.append({
                    'Pattern': pattern_label,
                    'Frequency': freq,
                    'Day': day,
                    'Truck Name': truck_name,
                    'Slot': slot_label,
                    'Agency Number': appt['agency_number'],
                    'Account Name': appt['account_name'],
                    'Area': appt['area'],
                    'Minimum Weight': appt['min_weight'],
                    'Maximum Weight': appt['max_weight'],
                    'Start Time': appt['start_time_str'],
                    'End Time': appt['end_time_str']
                })
    if not out_rows:
        flash("No recurring assignments to download.", "error")
        return redirect(url_for('recurring_schedule'))

    df_out = pd.DataFrame(out_rows)
    cols = ['Pattern','Frequency','Day','Truck Name','Slot',
            'Agency Number','Account Name','Area',
            'Minimum Weight','Maximum Weight','Start Time','End Time']
    df_out = df_out[cols]
    csv_text = df_out.to_csv(index=False)
    buf = BytesIO(csv_text.encode('utf-8'))
    buf.seek(0)
    return send_file(
        buf,
        mimetype='text/csv',
        as_attachment=True,
        download_name='recurring_schedule.csv'
    )


# ----------------------
# Run the app
# ----------------------
if __name__ == '__main__':
    # For development; in production, use a WSGI server
    app.run(debug=True)
