"""Regresión del formulario: ejecutar dentro de la imagen Airflow para validarlo."""
import copy
import sys
import unittest
from pathlib import Path

try:
    from airflow.sdk import Param
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


@unittest.skipUnless(AIRFLOW_AVAILABLE, 'Requiere el entorno Airflow para validar su Param real')
class OptionalSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'airflow/dags'))
        from train_star_model import dag
        cls.dag=dag

    def parameter(self):
        return copy.deepcopy(self.dag.params.get_param('snapshot_sha256'))

    def test_default_is_null(self):
        self.assertIsNone(self.parameter().resolve())

    def test_empty_form_value_is_allowed(self):
        self.assertIsNone(self.parameter().resolve(value=None))

    def test_explicit_hash_remains_valid(self):
        value='a1'*32
        self.assertEqual(self.parameter().resolve(value=value),value)

    def test_malformed_hash_remains_invalid(self):
        from airflow.exceptions import ParamValidationError
        for value in ['abc','z'*64,42]:
            with self.subTest(value=value),self.assertRaises(ParamValidationError):
                self.parameter().resolve(value=value)


if __name__=='__main__':unittest.main()
