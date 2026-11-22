"""Entrenamiento con seis etapas independientes y un entorno preinstalado."""
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag
from airflow.sdk import Param
from airflow.providers.standard.operators.python import ExternalPythonOperator


# ExternalPythonOperator serializa cada callable: sus imports viven dentro de él.
def _resolve(execution_id, config):
    import sys
    sys.path.insert(0,'/opt/airflow/dags')
    from star_training.orchestration import resolve_snapshot
    return resolve_snapshot(execution_id,config)


def _search(reference):
    import sys
    sys.path.insert(0,'/opt/airflow/dags')
    from star_training.orchestration import search_stage
    return search_stage(reference)


def _train(reference, selected):
    import sys
    sys.path.insert(0,'/opt/airflow/dags')
    from star_training.orchestration import train_stage
    return train_stage(reference,selected)


def _evaluate(reference, trained):
    import sys
    sys.path.insert(0,'/opt/airflow/dags')
    from star_training.orchestration import evaluation_stage
    return evaluation_stage(reference,trained)


def _register(reference, evaluated):
    import sys
    sys.path.insert(0,'/opt/airflow/dags')
    from star_training.orchestration import registration_stage
    return registration_stage(reference,evaluated)


def _promote(reference, candidate):
    import sys
    import json
    import logging
    sys.path.insert(0,'/opt/airflow/dags')
    from star_training.orchestration import promotion_stage
    result=promotion_stage(reference,candidate)
    logging.info('Comparación y promoción: %s',json.dumps(result,ensure_ascii=False))
    return result


@dag(
    dag_id='train_star_model',
    description='Entrena D2, registra candidato y compara/promueve champion en una tarea separada',
    start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=None,catchup=False,
    max_active_runs=1,max_active_tasks=1,render_template_as_native_obj=True,
    default_args={'owner':'rmb','retries':2,'retry_delay':timedelta(minutes=1),
                  'execution_timeout':timedelta(minutes=20)},
    params={
        'n_trials':Param(250,type='integer',minimum=1,maximum=1000,
                         description='Trials Optuna; usar 2 o 5 para una prueba rápida'),
        'seed':Param(42,type='integer',minimum=0,maximum=4294967295),
 'snapshot_sha256': Param(
    None,
    type=['null', 'string'],
    pattern='^([0-9a-f]{64})?$',
    description='Vacío: última ingesta exitosa; opcional: hash concreto',
),
        'min_delta':Param(0.0,type='number',minimum=0,maximum=1,
                          description='Mejora mínima de F1 weighted; se exige una diferencia estrictamente mayor'),
    },
    tags=['training','SDSS','star type'],
    doc_md='''Después de `process_etl_star_data`: snapshot S3, Optuna CV5, Pipeline
D2 y registro sin cambiar el alias hasta la última tarea independiente.
`compare_and_promote_champion` evalúa ambas versiones en el mismo test: mejora
estricta mueve champion; empate o peor conserva. Sin champion, promueve el primero
válido. Revisar `promotion/comparison.json` y XCom final. Checkpoints en
`s3://data/training/star/<execution_hash>/`. No ejecutar otros promotores del alias
simultáneamente. Todas las tareas usan un entorno instalado durante el build.''',
)
def train_star_model():
    common={'python':'/home/airflow/star-venv/bin/python','expect_airflow':False,
            'env_vars':{'MPLBACKEND':'Agg','MLFLOW_DISABLE_AGENT_HINT':'1'}}
    reference=ExternalPythonOperator(
        task_id='resolve_snapshot',python_callable=_resolve,**common,
        op_kwargs={'execution_id':'{{ dag.dag_id }}:{{ run_id }}',
                   'config':"{{ {'n_trials': params.n_trials, 'seed': params.seed, "
                            "'snapshot_sha256': params.snapshot_sha256, 'min_delta': params.min_delta} }}"})
    selected=ExternalPythonOperator(
        task_id='search_hyperparameters',python_callable=_search,**common,
        execution_timeout=timedelta(hours=2),op_kwargs={'reference':reference.output})
    trained=ExternalPythonOperator(
        task_id='train_and_log_model',python_callable=_train,**common,
        op_kwargs={'reference':reference.output,'selected':selected.output})
    evaluated=ExternalPythonOperator(
        task_id='evaluate_candidate',python_callable=_evaluate,**common,
        op_kwargs={'reference':reference.output,'trained':trained.output})
    candidate=ExternalPythonOperator(
        task_id='register_candidate',python_callable=_register,**common,
        op_kwargs={'reference':reference.output,'evaluated':evaluated.output})
    ExternalPythonOperator(
        task_id='compare_and_promote_champion',python_callable=_promote,**common,
        op_kwargs={'reference':reference.output,'candidate':candidate.output})


dag=train_star_model()
