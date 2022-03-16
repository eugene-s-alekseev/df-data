from src.config import metadata_engine


def main():
    connection = metadata_engine.connect()
    transaction = connection.begin()
    try:
        connection.execute("""
        drop table if exists app_info;
        """)
        connection.execute("""
        create table if not exists app_info (
        app_id varchar(60) primary key,
        app_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists draft_info;
        """)
        connection.execute("""
        create table if not exists draft_info (
        draft_id varchar(60) primary key,
        draft_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists source_info;
        """)
        connection.execute("""
        create table if not exists source_info (
        source_id varchar(60) primary key,
        source_name varchar(60) not null unique,
        connection_info json not null
        );
        """)

        connection.execute("""
        drop table if exists processor_backend_info;
        """)
        connection.execute("""
        create table if not exists processor_backend_info (
        processor_backend_id varchar(60) primary key,
        processor_backend_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists driver_info;
        """)
        connection.execute("""
        create table if not exists driver_info (
        driver_id varchar(60) primary key,
        driver_name varchar(60) not null unique,
        driver_function varchar(60) not null,
        location_info json not null,
        source_id varchar(60) not null references source_info(source_id) on delete restrict,
        processor_backend_id varchar(60) not null references processor_backend_info(processor_backend_id) on delete restrict,
        constraint driver_function_check check (driver_function in ('on_read', 'on_write'))
        );
        """)

        connection.execute("""
        drop table if exists dataset_implementation_info;
        """)
        connection.execute("""
        create table if not exists dataset_implementation_info (
        dataset_implementation_id varchar(60) primary key,
        dataset_implementation_name varchar(60) not null unique,
        app_id varchar(60) not null references app_info(app_id) on delete restrict,
        draft_id varchar(60) not null references draft_info(draft_id) on delete restrict,
        source_id varchar(60) not null references source_info(source_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists abstract_processor_info;
        """)
        connection.execute("""
        create table if not exists abstract_processor_info (
        abstract_processor_id varchar(60) primary key,
        abstract_processor_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists processor_implementation_info;
        """)
        connection.execute("""
        create table if not exists processor_implementation_info (
        processor_implementation_id varchar(60) primary key,
        processor_implementation_name varchar(60) not null unique,
        implements_abstract_processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
        backend_id varchar(60) not null references processor_backend_info(processor_backend_id)
        );
        """)

        connection.execute("""
        drop table if exists processor_upon_processor_dependency_info;
        """)
        connection.execute("""
        create table if not exists processor_upon_processor_dependency_info (
        processor_upon_processor_dependency_id varchar(60) primary key,
        processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
        depends_on_processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists processor_upon_primary_source_dependency_info;
        """)
        connection.execute("""
        create table if not exists processor_upon_primary_source_dependency_info (
        processor_upon_primary_source_dependency_id varchar(60) primary key,
        processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
        depends_on_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists processor_produces_sink_info;
        """)
        connection.execute("""
        create table if not exists processor_produces_sink_info (
        processor_produces_sink_id varchar(60) primary key,
        processor_id varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
        produces_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists filled_drafts_info;
        """)
        connection.execute("""
        create table if not exists filled_drafts_info (
        dataset_implementation_id varchar(60) primary key references dataset_implementation_info(dataset_implementation_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists dataset_implementation_priorities;
        """)
        connection.execute("""
        create table if not exists dataset_implementation_priorities (
        dataset_implementation_id varchar(60) references dataset_implementation_info(dataset_implementation_id) on delete restrict,
        function varchar(60) not null,
        priority int not null,
        constraint priorities_function_check check (function in ('on_read', 'on_write'))
        );
        """)

        connection.execute("""
        drop table if exists dag_flow_level_info;
        """)
        connection.execute("""
        create table if not exists dag_flow_level_info (
        dag_flow_level_id varchar(60) primary key,
        dag_flow_level_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists abstract_processor_to_implementation_mapping_info;
        """)
        connection.execute("""
        create table if not exists abstract_processor_to_implementation_mapping_info (
        abstract_processor_to_implementation_mapping_id varchar(60) not null,
        abstract_processor_id                           varchar(60) not null references abstract_processor_info(abstract_processor_id) on delete restrict,
        processor_implementation_id                     varchar(60) not null references processor_implementation_info(processor_implementation_id) on delete restrict,
        primary key (abstract_processor_to_implementation_mapping_id, abstract_processor_id, processor_implementation_id)
        );
        """)

        connection.execute("""
        drop table if exists dag_implementation_info;
        """)
        connection.execute("""
        create table if not exists dag_implementation_info (
        dag_implementation_id varchar(60) primary key,
        dag_implementation_name varchar(60) not null unique,
        implements_dag_flow_level_id varchar(60) not null references dag_flow_level_info(dag_flow_level_id) on delete restrict,
        abstract_processor_to_implementation_mapping_id varchar(60) not null
        references abstract_processor_to_implementation_mapping_info(abstract_processor_to_implementation_mapping_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists dag_build_info;
        """)
        connection.execute("""
        create table if not exists dag_build_info (
        dag_build_id varchar(60) primary key,
        dag_build_name varchar(60) not null unique,
        app_id varchar(60) not null references app_info(app_id) on delete restrict,
        dag_implementation_id varchar(60) not null references dag_implementation_info(dag_implementation_id) on delete restrict,
        build_parameters json not null
        );
        """)

        connection.execute("""
        drop table if exists abstract_model_info;
        """)
        connection.execute("""
        create table if not exists abstract_model_info (
        abstract_model_id varchar(60) primary key,
        abstract_model_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists model_upon_inlet_draft_dependency_info;
        """)
        connection.execute("""
        create table if not exists model_upon_inlet_draft_dependency_info (
        model_upon_inlet_draft_dependency_id varchar(60) primary key,
        abstract_model_id varchar(60) not null references abstract_model_info(abstract_model_id) on delete restrict,
        depends_on_inlet_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists model_produces_outlet_draft_info;
        """)
        connection.execute("""
        create table if not exists model_produces_outlet_draft_info (
        model_produces_outlet_draft_id varchar(60) primary key,
        abstract_model_id varchar(60) not null references abstract_model_info(abstract_model_id) on delete restrict,
        produces_outlet_draft_id varchar(60) not null references draft_info(draft_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists model_backend_info;
        """)
        connection.execute("""
        create table if not exists model_backend_info (
        model_backend_id varchar(60) primary key,
        model_backend_name varchar(60) not null unique
        );
        """)

        connection.execute("""
        drop table if exists model_implementation_info;
        """)
        connection.execute("""
        create table if not exists model_implementation_info (
        model_implementation_id varchar(60) primary key,
        model_implementation_name varchar(60) not null unique,
        implements_abstract_model_id varchar(60) not null references abstract_model_info(abstract_model_id) on delete restrict,
        model_backend_id varchar(60) not null references model_backend_info(model_backend_id) on delete restrict
        );
        """)

        connection.execute("""
        drop table if exists model_backends_available;
        """)
        connection.execute("""
        create table if not exists model_backends_available (
        processor_backend_id varchar(60) not null,
        model_backend_id varchar(60) not null,
        primary key (processor_backend_id, model_backend_id)
        );
        """)

        connection.execute("""
        drop table if exists model_driver_info;
        """)
        connection.execute("""
        create table if not exists model_driver_info (
        model_driver_id varchar(60) primary key,
        model_driver_name varchar(60) not null unique,
        processor_backend_id varchar(60) not null references processor_backend_info(processor_backend_id) on delete restrict,
        model_backend_id varchar(60) not null references model_backend_info(model_backend_id) on delete restrict,
        function varchar(60) not null,
        constraint driver_function_check check (function in ('on_read', 'on_write'))
        );
        """)

        connection.execute("""
        drop table if exists model_performance_info;
        """)
        connection.execute("""
        create table if not exists model_performance_info (
        model_implementation_id varchar(60) not null references model_implementation_info(model_implementation_id) on delete restrict,
        metric varchar(60) not null,
        value float not null,
        primary key (model_implementation_id, metric)
        );
        """)

        transaction.commit()
    except:
        transaction.rollback()
        print("Error: failed to define metadata; something went wrong.")
        raise

    connection.close()


if __name__ == "__main__":
    main()
