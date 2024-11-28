from airflow import settings
from airflow.models import DagBag
from airflow.utils.db import provide_session
from flask_appbuilder.security.sqla.models import Role, PermissionView
from airflow.www.security import AirflowSecurityManager

# Define a mapping between tags and roles
TAG_TO_ROLE_MAPPING = {
    "sample": "healthcheck_role",
    "objects": "objects_role",
}

@provide_session
def sync_roles_with_dag_tags(session=None):
    """
    Sync roles and permissions based on DAG tags.
    """
    dagbag = DagBag()
    security_manager = AirflowSecurityManager()

    for dag_id, dag in dagbag.dags.items():
        tags = dag.tags  # Get tags for the DAG

        for tag in tags:
            role_name = TAG_TO_ROLE_MAPPING.get(tag)
            if not role_name:
                print(f"Warning: No role found for tag '{tag}'")
                continue

            # Check if the role exists
            role = session.query(Role).filter_by(name=role_name).first()
            if not role:
                # Create the role if it doesn't exist
                role = Role(name=role_name)
                session.add(role)

            # Grant permissions for the DAG
            for permission_name in ["can_read", "can_edit", "can_delete"]:
                # Check if the permission already exists
                permission_view = session.query(PermissionView).filter_by(
                    permission=permission_name, view_menu=f"DAG:{dag_id}"
                ).first()

                if not permission_view:
                    # Create the permission if it doesn't exist
                    security_manager.add_permission_view_menu(permission_name, f"DAG:{dag_id}")
                    permission_view = session.query(PermissionView).filter_by(
                        permission=permission_name, view_menu=f"DAG:{dag_id}"
                    ).first()

                # Associate the permission with the role
                if permission_view not in role.permissions:
                    role.permissions.append(permission_view)

    session.commit()
