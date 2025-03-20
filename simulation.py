#ipynb working: test2.ipynb 

from uxsim import *
import random
import json

# 1.1 Simulation Setup and Run 
seed = None

W = World(
    name="",
    deltan=5,
    tmax=3600, #1hour simulation
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)

random.seed(seed)

#--------- NETWORK DEFINITION ----------#
'''
  N1  
    |   
    I1--I2--E1
    |   |
W1--I3--I4  
        |
        S1
'''

#NODES & SIGNALS#
EWsignal = 30
NSsignal = 30

N1 = W.addNode("N1", -2, 4)
W1 = W.addNode("W1", -4, -2)
S1 = W.addNode("S1", 2, -4)
E1 = W.addNode("E1", 4, 2)

I1 = W.addNode("I1", -2, 2, signal=[EWsignal, NSsignal])
I2 = W.addNode("I2", 2, 2, signal=[EWsignal, NSsignal])
I3 = W.addNode("I3", -2, -2, signal=[EWsignal, NSsignal])
I4 = W.addNode("I4", 2, -2, signal=[EWsignal, NSsignal])

#LINKS#
#E<-->W direstion: signal group 0
for n1, n2 in [[E1, I2], [I2, E1], [I2, I1], [W1, I3], [I3, W1], [I3,I4]]:
    W.addLink(n1.name+n2.name, n1, n2, length=200, free_flow_speed=50, jam_density=0.2, number_of_lanes=1, signal_group=0)
#N<-->S direction: signal group 1
for n1, n2 in [[N1, I1], [I1, N1], [I1, I3], [S1, I4], [I4, S1], [I4,I2]]:
    W.addLink(n1.name+n2.name, n1, n2, length=200, free_flow_speed=50, jam_density=0.2, number_of_lanes=1, signal_group=1)

#DEMANDS#
# random demand definition every 30 seconds
dt = 30
demand = 2 #average demand for the simulation time
demands = []
for t in range(0, 3600, dt):
    for n1, n2 in [[N1, S1], [N1, W1], [N1, E1]]:
        dem = random.uniform(0, demand)
        W.adddemand(n1, n2, t, t+dt, dem)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})
    for n1, n2 in [[S1, W1], [S1, N1], [S1, E1]]:
        dem = random.uniform(0, demand)
        W.adddemand(n1, n2, t, t+dt, dem)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})
    for n1, n2 in [[E1, W1], [E1, N1], [E1, S1]]:
        dem = random.uniform(0, demand)
        W.adddemand(n1, n2, t, t+dt, dem)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})
    for n1, n2 in [[W1, E1], [W1, N1], [W1, S1]]:
        dem = random.uniform(0, demand)
        W.adddemand(n1, n2, t, t+dt, dem)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})

W.exec_simulation()
W.analyzer.print_simple_stats()

# Simulation animation
W.analyzer.network_anim(detailed=0, network_font_size=0)

#Transform in json
df = W.analyzer.vehicles_to_pandas()
res = df.to_json(orient="index")
jres = json.loads(res)

#Print and Export results
df.to_json('./out/simres.json', orient="index") #make sure you're in ~/BigDataProject
df.to_csv('./out/simres.csv')
