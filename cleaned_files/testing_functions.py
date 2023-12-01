import unittest
import testing_teams as tt
import testing_gamelogs as tg

class TestTeams(unittest.TestCase):
    def test_team_count(self):
        mlb_team_count = 30
        testing_teams = tt.rows
        self.assertEqual(testing_teams,mlb_team_count)

    def testing_gamelogs(self):
        away_games_per_year = 81
        home_games_per_year = 81
        testing_gamelogs_games_per_year = tg.rows_away+tg.rows_home
        self.assertEqual(testing_gamelogs_games_per_year,home_games_per_year+away_games_per_year)

if __name__ == '__main__':
    unittest.main()
