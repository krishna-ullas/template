from airflow import settings
from airflow.models import DagBag
from airflow.utils.db import provide_session
from flask_appbuilder.security.sqla.models import Role, PermissionView
from sqlalchemy import Table
from sqlalchemy.ext.declarative import declared_attr

# Define a mapping between tags and roles
TAG_TO_ROLE_MAPPING = {
    "objects": "objects_role",
    "sample": "healthcheck_role",
}

# Function to sync roles with DAG tags
@provide_session
def sync_roles_with_dag_tags(session=None):
    role_table = Table("ab_role", Role.metadata, extend_existing=True)
    permission_view_table = Table("ab_permission_view", PermissionView.metadata, extend_existing=True)
    # Load all DAGs in the system
    dagbag = DagBag()

    # Loop through each DAG in the system
    for dag_id, dag in dagbag.dags.items():
        tags = dag.tags  # Get tags for this DAG

        # For each tag in the DAG
        for tag in tags:
            # Find the corresponding role from the mapping
            role_name = TAG_TO_ROLE_MAPPING.get(tag)
            if not role_name:
                # Skip if no role is defined for the tag
                print(f"Warning: No role found for tag '{tag}'")
                continue

            # Check if the role already exists
            role = session.query(Role).filter_by(name=role_name).first()
            if not role:
                # If not, create the role
                role = Role(name=role_name)
                session.add(role)

            # Grant "read" permission to the DAG for the role
            read_permission = session.query(Permission).filter_by(action="can_read", resource=f"DAG:{dag_id}").first()
            if not read_permission:
                read_permission = Permission(action="can_read", resource=f"DAG:{dag_id}")
                session.add(read_permission)
            if read_permission not in role.permissions:
                role.permissions.append(read_permission)

            # Grant "edit" permission to the DAG for the role
            edit_permission = session.query(Permission).filter_by(action="can_edit", resource=f"DAG:{dag_id}").first()
            if not edit_permission:
                edit_permission = Permission(action="can_edit", resource=f"DAG:{dag_id}")
                session.add(edit_permission)
            if edit_permission not in role.permissions:
                role.permissions.append(edit_permission)

            # Grant "delete" permission to the DAG for the role
            delete_permission = session.query(Permission).filter_by(action="can_delete", resource=f"DAG:{dag_id}").first()
            if not delete_permission:
                delete_permission = Permission(action="can_delete", resource=f"DAG:{dag_id}")
                session.add(delete_permission)
            if delete_permission not in role.permissions:
                role.permissions.append(delete_permission)

    # Commit the changes to the database
    session.commit()

# Define the DAG
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 1,
}
with DAG(
    "sync_roles_permissions_dag",
    default_args=default_args,
    description="Sync roles and permissions based on DAG tags",
    schedule_interval=None,  # Run on demand
    start_date=days_ago(1),
    tags=["sync", "roles", "permissions"],
) as dag:
    # Add a PythonOperator to run the sync function
    sync_task = PythonOperator(
        task_id="sync_roles_permissions_task",
        python_callable=sync_roles_with_dag_tags,
    )
