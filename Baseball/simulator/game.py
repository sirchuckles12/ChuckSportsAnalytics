import numpy as np
from player import DistributionBlender

class Game:
    def __init__(self, home_batting_lineup, away_batting_lineup, home_pitching_lineup, away_pitching_lineup
                 , half_inning = 1, home_score = 0, away_score = 0, on_base = [], outs = 0
                 , home_batter_idx = 0, away_batter_idx = 0, home_pitcher_idx = 0, away_pitcher_idx = 0):
        self.half_inning = half_inning
        self.home_score = home_score
        self.away_score = away_score
        self.on_base = on_base
        self.outs = outs
        self.home_batting_lineup = home_batting_lineup
        self.away_batting_lineup = away_batting_lineup
        self.home_batter_idx = home_batter_idx
        self.away_batter_idx = away_batter_idx
        self.home_pitching_lineup = home_pitching_lineup
        self.away_pitching_lineup = away_pitching_lineup
        self.home_pitcher_idx = home_pitcher_idx
        self.away_pitcher_idx = away_pitcher_idx
        self.is_complete = False
        self.at_bats = []
    
    def update(self, ab_outs = 0, is_hit = 0, is_walk = 0, hit_base = 0, outcome = 'out'):#, **kwargs):
        
        if self.outs + ab_outs >= 3:
            inning_end = True
        else:
            inning_end = False

        self.at_bats.append(self.summarize_game_state(inning_end = inning_end, ab_outs = ab_outs
                                                , is_hit = is_hit, is_walk = is_walk
                                                , hit_base = hit_base, outcome = outcome))
                        
        if is_walk:
            if 1 in self.on_base:
                self.on_base = [x + 1 for x in self.on_base] + [1]
                self.update_score()
            else:
                self.on_base = self.on_base + [1]
                
        if is_hit:                
            self.on_base = [x + hit_base + 1 for x in self.on_base] + [hit_base]            
            self.update_score()
        
        #increment batter
        if self.half_inning % 2 == 0:
            self.home_batter_idx += 1
            if self.home_batter_idx > 8:
                self.home_batter_idx = 0
        else: 
            self.away_batter_idx += 1
            if self.away_batter_idx > 8:
                self.away_batter_idx = 0

        if ab_outs:
            self.outs += ab_outs
            if self.outs >= 3:
                self.half_inning += 1
                self.outs = 0
                self.on_base = []

        self.update_pitcher()
        self.check_game_end()

    def update_score(self):
        runs_scored = np.sum([x >= 4 for x in self.on_base])
        if runs_scored > 0:
            self.get_current_batter().rbis += runs_scored
            if self.half_inning % 2 == 0:
                self.home_score += runs_scored
                self.on_base = [x for x in self.on_base if x < 4]
            else: 
                self.away_score += runs_scored
                self.on_base = [x for x in self.on_base if x < 4]
        return True

    def get_current_pitcher(self):
        if self.half_inning % 2 == 0:
            return self.away_pitching_lineup[self.away_pitcher_idx]
        else:
            return self.home_pitching_lineup[self.home_pitcher_idx]

    def get_current_batter(self):
        if self.half_inning % 2 == 0:
            return self.home_batting_lineup[self.home_batter_idx]
        else:
            return self.away_batting_lineup[self.away_batter_idx]

    def update_pitcher(self):
        p = self.get_current_pitcher()
        if p.pitches > 90:
            if self.half_inning % 2 == 0:
                self.away_pitcher_idx = 1
            else:
                self.home_pitcher_idx = 1
    
    def get_next_atbat(self):
        return AtBat(batter = self.get_current_batter(), pitcher = self.get_current_pitcher())
    
    def check_game_end(self):
        if self.half_inning >= 19:
            self.is_complete = True
#        if self.half_inning < 17:
#            return True
#        elif self.half_inning >= 18 and self.home_score > self.away_score and self.half_inning % 2 == 0:
#            self.is_complete = True
#        if self.half_inning == 19 and self.away_score > self.home_score:
#            self.is_complete = True
#        if self.half_inning >

    def summarize_game_state(self, inning_end = False, ab_outs = 0
                       , is_hit = 0, is_walk = 0
                       , hit_base = 0, outcome = 'out'):
        return({'half_inning': self.half_inning, 'home_score_start': self.home_score
                , 'away_score_start': self.away_score, 'on_base_start': self.on_base, 'out_start': self.outs
                , 'pitcher': self.get_current_pitcher().name, 'batter': self.get_current_batter().name
                , 'inning_end': inning_end, 'outcome': outcome
                , 'is_hit': is_hit, 'is_walk': is_walk, 'hit_base': hit_base, 'ab_outs': ab_outs})

    def play_game(self):
        while self.is_complete is False:
            a = self.get_next_atbat()
            o = a.get_outcome()
            self.update(**o)
        
    def __str__(self):
        return str([(attr, getattr(self,attr)) for attr in dir(self) if not callable(getattr(self,attr)) and not attr.startswith("__") and 'lineup' not in attr and 'idx' not in attr])
    

class AtBat:
    def __init__(self, batter, pitcher):
        self.batter = batter
        self.pitcher = pitcher
        
    def get_outcome(self):
        pitch = self.pitcher.get_outcome_distribution()
        bat = self.batter.get_outcome_distribution()
        dist = DistributionBlender(pitch, bat, outcome_col = 'outcome', prob_col = 'probability')
        outcome = dist.sample()
        self.pitcher.pitches += self.pitcher.get_pitches_distribution(outcome).sample()
        
        if outcome == 'strikeout':
            self.pitcher.strikeouts += 1
            self.pitcher.outs += 1
            return({'outcome': outcome, 'ab_outs': 1})
        elif outcome == 'walk': 
            self.pitcher.walks += 1
            self.batter.walks += 1
            return({'outcome': outcome, 'ab_outs': 0, 'is_walk': 1})
        elif outcome == 'out': 
            self.pitcher.outs += 1
            return({'outcome': outcome, 'ab_outs': 1})
        elif outcome == 'hit':
            self.pitcher.hits += 1
            self.batter.hits += 1
            hit_base = self.batter.get_base_distribution().sample()
            
            if hit_base == 1:
                self.batter.singles += 1
            elif hit_base == 2:
                self.batter.doubles += 1
            elif hit_base == 3:
                self.batter.triples += 1
            elif hit_base == 4:
                self.batter.hrs += 1
            else:
                print(hit_base)
                
            return({'outcome': outcome, 'is_hit': 1, 'hit_base': hit_base})
            
    def __str__(self):
        return str([(attr, getattr(self,attr)) for attr in dir(self) if not callable(getattr(self,attr)) and not attr.startswith("__") and 'lineup' not in attr])