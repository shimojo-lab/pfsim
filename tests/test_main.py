from logging import DEBUG, INFO, getLogger
from unittest import TestCase
from unittest.mock import patch

from docopt import DocoptExit

from pfsim import main


class TestMain(TestCase):
    def test_usage(self):
        with patch("sys.argv", "__main__.py".split()):
            with self.assertRaises(DocoptExit):
                main()

        with patch("sys.argv", "__main__.py -h".split()):
            with self.assertRaises(SystemExit):
                main()

    @patch("pfsim.Experiment")
    def test_log_level(self, MockExperiment):
        with patch("sys.argv", "__main__.py foo.yml".split()):
                main()
                assert getLogger().getEffectiveLevel() == INFO
        with patch("sys.argv", "__main__.py -v foo.yml".split()):
                main()
                assert getLogger().getEffectiveLevel() == DEBUG

    @patch("pfsim.Experiment")
    def test_experiment(self, MockExperiment):
        with patch("sys.argv", "__main__.py bar.yml".split()):
                main()
                MockExperiment.assert_called_once_with("bar.yml")
