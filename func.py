import io
import os
import json
import oracledb
from timeit import default_timer as timer
from fdk import response

# Get connection parameters from enviroment
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
dsn = os.getenv("DSN")

if db_user == None:
    raise ValueError("ERROR: Missing configuration key DBUSER")
if db_password == None:
    raise ValueError("ERROR: Missing configuration key DBPASSWORD")
if dsn == None:
    raise ValueError("ERROR: Missing configuration key DSN")

print("INFO: db_user is {}".format(db_user), flush=True)
print("INFO: db_password is {}".format(db_password), flush=True)
print("INFO: dsn is {}".format(dsn), flush=True)


# Create the DB Session Pool
start_pool = timer()
dbpool = oracledb.create_pool(user=db_user, password=db_password, dsn=dsn, min=1, max=10) 
end_pool = timer()
print("INFO: DB pool created in {} sec".format(end_pool - start_pool), flush=True)


#
# Function Handler: executed every time the function is invoked
#
def handler(ctx, data: io.BytesIO = None):
    try:
        method = ctx.Method()
        if method == 'POST':
            return handle_post(ctx, data)
        elif method == 'GET':
            return handle_get(ctx, data)
        elif method == 'PUT':
            return handle_put(ctx, data)
        elif method == 'DELETE':
            return handle_delete(ctx, data)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise
    
def handle_post(ctx, data: io.BytesIO = None):
    try:
        payload_bytes = data.getvalue()
        if payload_bytes == b'':
            raise KeyError('No keys in payload')
        payload = json.loads(payload_bytes)
        
        user_id = ''
        path = ctx.RequestURL()
        print("INFO: Url path is parsed as {}".format(path), flush=True)
        path_parts = path.strip('/').split('/')
        print("INFO: path_parts parsed", flush=True)
        if len(path_parts) >= 2 and path_parts[-2] == 'users':
            user_id = path_parts[-1]
            print("INFO: User ID is parsed as {}".format(user_id), flush=True)

        if user_id == '':
            raise ValueError("Missing required fields: user_id")

        print(payload, flush=True)
        first_name = payload.get("first_name")
        last_name = payload.get("last_name")
        username = payload.get("username")
        print("INFO: Mid", flush=True)
        if not first_name or not last_name or not username:
            raise ValueError("Missing required fields: first_name, last_name, username")

        print("INFO: Paramters has been successfully parsed")
        sql_statement = """
            INSERT INTO users (ID, FIRST_NAME, LAST_NAME, USERNAME)
            VALUES (:1, :2, :3, :4)
        """
        bind_vars = [user_id, first_name, last_name, username]

    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise

    try:
        with dbpool.acquire() as dbconnection:
            print("INFO: DB connections has been acquired")
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbconnection.commit()
                return response.Response(
                    ctx,
                    response_data=json.dumps({"message": "User created successfully"}),
                    headers={"Content-Type": "application/json"}
                )
    except Exception as ex:
        print('ERROR: Failed to create user', ex, flush=True)
        raise

def handle_get(ctx, data: io.BytesIO = None):
    try:
        user_id = None
        path = ctx.RequestURL()
        path_parts = path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[-2] == 'users':
            user_id = path_parts[-1]

        if not user_id:
            return read_all_users(ctx)
        else:
            return read_user(ctx, user_id)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise
    

def read_user(ctx, user_id):
    try:
        sql_statement = """
            SELECT ID, FIRST_NAME, LAST_NAME, USERNAME, CREATED_ON
            FROM users
            WHERE ID = :1
        """
        bind_vars = [user_id]

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbcursor.rowfactory = lambda *args: dict(zip([d[0] for d in dbcursor.description], args))
                result = dbcursor.fetchone()

                if result:
                    if result.get("CREATED_ON"):
                        result["CREATED_ON"] = result["CREATED_ON"].isoformat()

                    return response.Response(
                        ctx,
                        response_data=json.dumps(result),
                        headers={"Content-Type": "application/json"}
                    )
                else:
                    return response.Response(
                        ctx,
                        response_data=json.dumps({"message": "User not found"}),
                        headers={"Content-Type": "application/json"}
                    )

    except Exception as ex:
        print('ERROR: Failed to read user', ex, flush=True)
        raise

def read_all_users(ctx):
    try:
        sql_statement = """
            SELECT ID, FIRST_NAME, LAST_NAME, USERNAME, CREATED_ON
            FROM users
        """

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement)
                dbcursor.rowfactory = lambda *args: dict(zip([d[0] for d in dbcursor.description], args))
                results = dbcursor.fetchall()

                for result in results:
                    if result.get("CREATED_ON"):
                        result["CREATED_ON"] = result["CREATED_ON"].isoformat()

                return response.Response(
                    ctx,
                    response_data=json.dumps(results),
                    headers={"Content-Type": "application/json"}
                )

    except Exception as ex:
        print('ERROR: Failed to read all users', ex, flush=True)
        raise

def handle_put(ctx, data: io.BytesIO = None):
    try:
        payload_bytes = data.getvalue()
        if payload_bytes == b'':
            raise KeyError('No keys in payload')
        payload = json.loads(payload_bytes)

        return update_user(ctx, payload)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise

def update_user(ctx, payload):
    try:
        user_id = None
        path = ctx.RequestURL()
        path_parts = path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[-2] == 'users':
            user_id = path_parts[-1]

        if not user_id:
            raise ValueError("Missing required fields: user_id")
        
        first_name = payload.get("first_name")
        last_name = payload.get("last_name")
        username = payload.get("username")

        sql_statement = """
            UPDATE users
            SET FIRST_NAME = :1, LAST_NAME = :2, USERNAME = :3
            WHERE ID = :4
        """
        bind_vars = [first_name, last_name, username, user_id]

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbconnection.commit()
                return response.Response(
                    ctx,
                    response_data=json.dumps({"message": "User updated successfully"}),
                    headers={"Content-Type": "application/json"}
                )

    except Exception as ex:
        print('ERROR: Failed to update user', ex, flush=True)
        raise
    
def handle_delete(ctx, data: io.BytesIO = None):
    try:
        user_id = None
        path = ctx.RequestURL()
        path_parts = path.strip('/').split('/')
        if len(path_parts) >= 2 and path_parts[-2] == 'users':
            user_id = path_parts[-1]

        if not user_id:
            raise ValueError("Missing required field: user_id")

        return delete_user(ctx, user_id)
    except Exception as ex:
        print('ERROR: Invalid payload', ex, flush=True)
        raise
    

def delete_user(ctx, user_id):
    try:

        sql_statement = """
            DELETE FROM users
            WHERE ID = :1
        """
        bind_vars = [user_id]

        with dbpool.acquire() as dbconnection:
            with dbconnection.cursor() as dbcursor:
                dbcursor.execute(sql_statement, bind_vars)
                dbconnection.commit()
                return response.Response(
                    ctx,
                    response_data=json.dumps({"message": "User deleted successfully"}),
                    headers={"Content-Type": "application/json"}
                )

    except Exception as ex:
        print('ERROR: Failed to delete user', ex, flush=True)
        raise
