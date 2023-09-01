import pandas as pd
import numpy as np
from db_connect import get_connection

engine = get_connection()

class DiscreteDistribution:
    
    def __init__(self, distribution, outcome_col = 'pitch_count', prob_col = 'probability'):
        self.distribution = distribution
        self.outcome_col = outcome_col
        self.prob_col = prob_col
    
    def sample(self):
        return np.random.choice(self.distribution[self.outcome_col].values, size = 1, p = self.distribution[self.prob_col].values)[0]

def simple_average_blender(x, pcol):
    return ((x['{}_one'.format(pcol)] + x['{}_two'.format(pcol)]) / 2)

class DistributionBlender(DiscreteDistribution):
    
    def __init__(self, dist_one, dist_two, outcome_col = 'pitch_count', prob_col = 'probability', mixing_function = simple_average_blender):
        self.dist_one = dist_one
        self.dist_two = dist_two
        self.outcome_col = outcome_col
        self.prob_col = prob_col
        self.mixing_function = mixing_function
        self.blend_distributions()
    
    def blend_distributions(self):
        a = self.dist_one.distribution
        b = self.dist_two.distribution
        c = a.merge(b, on = self.outcome_col, suffixes = ['_one', '_two'])
        c[self.prob_col] = c.apply(self.mixing_function, axis = 1, pcol = self.prob_col)
        c[self.prob_col] = c[self.prob_col] / sum(c[self.prob_col])
        self.distribution = c

class Pitcher:
    def __init__(self, name, pitches = 0, outs = 0, strikeouts = 0, walks = 0, hits = 0):
        self.real_name = name.replace('\'', '')
        self.name = self.real_name
        self.pitches = pitches
        self.outs = outs
        self.strikeouts = strikeouts
        self.walks = walks
        self.hits = hits
        self.pitch_distributions = {}
        self.compute_outcome_distribution()
        self.compute_pitch_distribution()
        
    def compute_outcome_distribution(self):
        tmp = pd.read_sql("""SELECT outcome, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE pitcher_name = '{}'
        AND outcome <> 'unknown'
        AND season >= 2023
        GROUP BY 1""".format(self.real_name), engine)
        
        if tmp.shape[0] == 0 or tmp['num_instances'].sum() < 100:
            tmp = pd.read_sql("""SELECT outcome, SUM(1) AS num_instances 
            FROM mlb_play_simulator_data
            WHERE outcome <> 'unknown'
            AND season >= 2023
            GROUP BY 1""", engine)
            self.name = 'Rookie'
#            raise ValueError('Pitcher Not Found')
        
        tmp['probability'] = tmp['num_instances'] / tmp['num_instances'].sum()
        self.outcome_distribution = DiscreteDistribution(tmp, outcome_col = 'outcome')
        
    def compute_pitch_distribution(self):
        tmp = pd.read_sql("""SELECT outcome, num_pitches, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE pitcher_name = '{}'
        AND outcome <> 'unknown'
        AND season >= 2023
        GROUP BY 1, 2""".format(self.real_name), engine)
        
        if tmp.shape[0] == 0 or tmp['num_instances'].sum() < 100:
            tmp = pd.read_sql("""SELECT outcome, num_pitches, SUM(1) AS num_instances 
            FROM mlb_play_simulator_data
            WHERE outcome <> 'unknown'
            AND season >= 2023
            GROUP BY 1, 2""", engine)
            self.name = 'Rookie'
        
        for outcome in tmp['outcome'].unique():
            o = tmp.query('outcome == \'{}\''.format(outcome)).copy()
            o['probability'] = o['num_instances'] / o['num_instances'].sum()
            self.pitch_distributions.update({outcome: DiscreteDistribution(o, outcome_col = 'num_pitches')})

    def get_outcome_distribution(self):
        return self.outcome_distribution

    def get_pitches_distribution(self, outcome):
        return self.pitch_distributions[outcome]
    
    def __str__(self):
        return str([(attr, getattr(self,attr)) for attr in dir(self) if not callable(getattr(self,attr)) and not attr.startswith("__") and 'distribution' not in attr])    

class Bullpen(Pitcher):
    def __init__(self, team, pitches = 0, outs = 0, strikeouts = 0, walks = 0, hits = 0):
        self.team = team
        self.name = team
        self.pitches = pitches
        self.outs = outs
        self.strikeouts = strikeouts
        self.walks = walks
        self.hits = hits
        self.pitch_distributions = {}
        self.compute_outcome_distribution()
        self.compute_pitch_distribution()
        
    #TODO: update these to be team specific?
    def compute_outcome_distribution(self):
        tmp = pd.read_sql("""SELECT outcome, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE outcome <> 'unknown'
        AND season >= 2023
        GROUP BY 1""", engine)
        
        if tmp.shape[0] == 0:
            raise ValueError('Pitcher Not Found')
        
        tmp['probability'] = tmp['num_instances'] / tmp['num_instances'].sum()
        self.outcome_distribution = DiscreteDistribution(tmp, outcome_col = 'outcome')
        
    #TODO: update these to be team specific?
    def compute_pitch_distribution(self):
        tmp = pd.read_sql("""SELECT outcome, num_pitches, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE outcome <> 'unknown'
        AND season >= 2023
        GROUP BY 1, 2""", engine)
        
        if tmp.shape[0] == 0:
            raise ValueError('Pitcher Not Found')
        
        for outcome in tmp['outcome'].unique():
            o = tmp.query('outcome == \'{}\''.format(outcome)).copy()
            o['probability'] = o['num_instances'] / o['num_instances'].sum()
            self.pitch_distributions.update({outcome: DiscreteDistribution(o, outcome_col = 'num_pitches')})
    
    
class Batter:
    def __init__(self, name, hits = 0, singles = 0, doubles = 0, triples = 0, hrs = 0, rbis = 0, walks = 0):
        self.real_name = name.replace('\'', '')
        self.name = self.real_name
        self.hits = hits
        self.singles = singles
        self.doubles = doubles
        self.triples = triples
        self.hrs = hrs
        self.rbis = rbis
        self.walks = walks
        self.pitch_distributions = {}
        self.compute_outcome_distribution()
        self.compute_pitch_distribution()
        self.compute_base_distribution()
    
    def compute_outcome_distribution(self):
        tmp = pd.read_sql("""SELECT outcome, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE batter_name = '{}'
        AND outcome <> 'unknown'
        AND season >= 2023
        GROUP BY 1""".format(self.real_name), engine)
        
        if tmp.shape[0] == 0 or tmp['num_instances'].sum() < 50:
            #print(self.real_name)
            #print(tmp['num_instances'].sum())
            tmp = pd.read_sql("""SELECT outcome, SUM(1) AS num_instances 
            FROM mlb_play_simulator_data
            WHERE outcome <> 'unknown'
            AND season >= 2023
            GROUP BY 1""", engine)
            self.name = 'Rookie'
        
        tmp['probability'] = tmp['num_instances'] / tmp['num_instances'].sum()
        self.outcome_distribution = DiscreteDistribution(tmp, outcome_col = 'outcome')
        
    def compute_pitch_distribution(self):
        tmp = pd.read_sql("""SELECT outcome, num_pitches, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE batter_name = '{}'
        AND outcome <> 'unknown'
        AND season >= 2023
        GROUP BY 1, 2""".format(self.real_name), engine)
        
        if tmp.shape[0] == 0 or tmp['num_instances'].sum() < 50:
            #print(self.real_name)
            #print(tmp['num_instances'].sum())
            tmp = pd.read_sql("""SELECT outcome, num_pitches, SUM(1) AS num_instances 
            FROM mlb_play_simulator_data
            WHERE outcome <> 'unknown'
            AND season >= 2023
            GROUP BY 1, 2""", engine)
            self.name = 'Rookie'
                
        for outcome in tmp['outcome'].unique():
            o = tmp.query('outcome == \'{}\''.format(outcome)).copy()
            o['probability'] = o['num_instances'] / o['num_instances'].sum()
            self.pitch_distributions.update({outcome: DiscreteDistribution(o, outcome_col = 'num_pitches')})
            
    def compute_base_distribution(self):
        tmp = pd.read_sql("""SELECT hit_bases, SUM(1) AS num_instances 
        FROM mlb_play_simulator_data
        WHERE batter_name = '{}'
        AND outcome = 'hit'
        AND season >= 2023
        GROUP BY 1""".format(self.real_name), engine)
        
        if tmp.shape[0] == 0 or tmp['num_instances'].sum() < 20:
#            print(self.real_name)
#            print(tmp['num_instances'].sum())
            tmp = pd.read_sql("""SELECT hit_bases, SUM(1) AS num_instances 
            FROM mlb_play_simulator_data
            WHERE outcome = 'hit'
            AND season >= 2023
            GROUP BY 1""", engine)
            self.name = 'Rookie'
        
        tmp['probability'] = tmp['num_instances'] / tmp['num_instances'].sum()
        self.base_distribution = DiscreteDistribution(tmp, outcome_col = 'hit_bases')

    def get_outcome_distribution(self):
        return (self.outcome_distribution)
    
    def get_base_distribution(self):
        return (self.base_distribution)
            
    def __str__(self):
        return str([(attr, getattr(self,attr)) for attr in dir(self) if not callable(getattr(self,attr)) and not attr.startswith("__") and 'distribution' not in attr])

    
