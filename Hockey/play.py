import pandas as pd
import requests

BASE = "http://statsapi.web.nhl.com/api/v1"

class Play():
    def __init__(self):
        self.event_idx = self.bundle['about']['eventIdx']
        self.period = self.bundle['about']['period']
        self.period_time = self.bundle['about']['periodTimeRemaining']
        self.event = self.bundle['result']['eventTypeId']
        self.description = self.bundle['result']['description']
        self.home_score = self.bundle['about']['goals']['home']
        self.away_score = self.bundle['about']['goals']['away']
        
    def to_frame(self):
        if self.loggable:
            #pdb.set_trace()
            return(pd.DataFrame({"event_idx": self.event_idx, "home_score": self.home_score
                                 , "away_score": self.away_score
                                 , "event": self.event, "period": self.period
                                 , "period_time": self.period_time
                                 , "description": self.description
                                 , "team": getattr(self, 'team', None)
                                 , "is_shot": getattr(self, 'is_shot', False)
                                 , "is_sog": getattr(self, 'is_sog', False)
                                 , "is_goal": getattr(self, 'is_goal', False)
                                 , "shooter": getattr(self, 'shooter', None)
                                 , "assist_one": getattr(self, 'assist_one', None)
                                 , "assist_two": getattr(self, 'assist_two', None)
                                 , "faceoff_winner": getattr(self, 'faceoff_winner', None)
                                 , "faceoff_loser": getattr(self, 'faceoff_loser', None)
                                 , 'goalie': getattr(self, 'goalie', None)
                    }, index = [1])
              )
        
class Shot(Play): 
    def __init__(self, bundle):
        self.bundle = bundle
        self.shooter = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Shooter']
        self.goalie = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Goalie'] or None
        self.is_shot = True
        self.is_sog = (self.bundle['result']['eventTypeId'] == 'SHOT')
        self.is_goal = False
        self.team = self.bundle['team']['name']
        self.loggable = True
        super().__init__()
        
    def __str__(self):
        return('{}, SOG: {}, Team: {},  Shooter: {}'.format(self.event, self.is_sog, self.team, self.shooter))

class Goal(Play): 
    def __init__(self, bundle):
        self.bundle = bundle
        self.shooter = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Scorer']
        self.assist_list = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Assist'] 
        self.goalie = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Goalie'] or None
        self.is_shot = True
        self.is_sog = True
        self.is_goal = True
        self.team = self.bundle['team']['name']
        self.loggable = True
        self.unpack_assists()
        super().__init__()
        
    def unpack_assists(self):
        if len(self.assist_list) == 0:
            pass
        if len(self.assist_list) == 1:
            self.assist_one = self.assist_list[0]
        if len(self.assist_list) == 2:
            self.assist_one = self.assist_list[0]
            self.assist_two = self.assist_list[1]
        
    def __str__(self):
        return('{}, Team: {}, Shooter: {}, Assists: {}'.format(self.event, self.team, self.shooter, self.assist_list))

class Penalty(Play):
    
    def __init__(self, bundle):
        self.bundle = bundle
        self.penalty_on = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'PenaltyOn']
        self.team = self.bundle['team']['name']
        self.loggable = True
        super().__init__()

    def __str__(self):
        return('{}, Team: {}, On: {}'.format(self.event, self.team, self.penalty_on))
    
class Faceoff(Play):
    
    def __init__(self, bundle):
        self.bundle = bundle
        self.faceoff_winner = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Winner']
        self.faceoff_loser = [a['player']['fullName'] for a in self.bundle['players'] if a['playerType'] == 'Loser']
        self.team = self.bundle['team']['name']
        self.loggable = False
        super().__init__()
        
    def __str__(self):
        return('{}, Team: {}, Winner: {}, Loser: {}'.format(self.event, self.team, self.faceoff_winner, self.faceoff_loser))
    
class Other(Play):
    
    def __init__(self, bundle):
        self.bundle = bundle
        self.loggable = False
        super().__init__()
        
    def __str__(self):
        return self.bundle['result']['description']
    

def process_play_bundle(b):
    
    event = b['result']['eventTypeId']
        
    if event in ['BLOCKED_SHOT', 'SHOT', 'MISSED_SHOT']:
        return Shot(b)
    
    if event in ['GOAL']:
        return Goal(b)
    
    if event in ['FACEOFF']:
        return Faceoff(b)
    
    if event in ['PENALTY']:
        return Penalty(b)

    if event in ['GAME_SCHEDULED', 'GIVEAWAY', 'TAKEAWAY', 'HIT', 'PERIOD_END', 
                'PERIOD_OFFICIAL', 'PERIOD_READY', 'PERIOD_START', 'STOP', 
                 'GAME_END', 'GAME_OFFICIAL', 'CHALLENGE', 'SHOOTOUT_COMPLETE']:
        return Other(b)
    else: 
        print("Weird event {}".format(event))
        return Other(b)

    
def process_game(game_id):
    
    s = requests.get('{}/game/{}/feed/live/'.format(BASE, game_id))
    g = s.json()['liveData']['plays']['allPlays']
        
    try:
        dat = pd.concat([process_play_bundle(a).to_frame() for a in g])
        dat = dat.sort_values('event_idx')
        return(dat)
    except:
        pass