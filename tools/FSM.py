#!/usr/bin/env python3.6
# coding: utf8

def doNothing(ctx, symbol, msg): #voir avec une lambda fonction
	pass

STATE_STD = 0
STATE_I = 1
STATE_F = 2
SYMBOL_EPSILON = ''

#==================================================
#==================================================

class State:
	def __init__(self, name, status = STATE_STD, action_in = doNothing, action_out = doNothing):
		self.name = name
		self.a_in = action_in
		self.a_out = action_out
		self.status = status

	def isInitial(self):
		return self.status in [1,3]

	def isFinal(self):
		return self.status in [2,3]

#==================================================

class InitialState(State):
	def __init__(self, name, action_in = doNothing, action_out = doNothing):
		State.__init__(self, name, status = STATE_I, action_in = doNothing, action_out = doNothing)

#==================================================

class FinalState(State):
	def __init__(self, name, action_in = doNothing, action_out = doNothing):
		State.__init__(self, name, status = STATE_F, action_in = doNothing, action_out = doNothing)

#==================================================

class InitialFinalState(State):
	def __init__(self, name, action_in = doNothing, action_out = doNothing):
		State.__init__(self, name, status = STATE_F+STATE_I, action_in = doNothing, action_out = doNothing)

#==================================================
#==================================================

class Transition:
	def __init__(self, symbol, sin, sout, action = doNothing):
		self.s_in = sin
		self.s_out = sout
		self.action = action
		self.symbol = symbol

	def isEpsilon(self):
		return self.symbol == SYMBOL_EPSILON

#==================================================

class EpsilonTransition(Transition):
	def __init__(self, sin, sout, action = doNothing):
		Transition.__init__(self, SYMBOL_EPSILON,sin, sout, action = doNothing)

#==================================================
#==================================================

class FSM:
	def __init__(self,A,Q, I, F, mu, ctx):
		self.A = A # set() of String or integers
		self.Q = Q # set() of States
		assert I in Q, 'Initial state is not a state'
		self.I =  I #None
		assert Q >= F, 'Final states are not a states'
		self.F = F # set()
		self._mu = dict()
		for t in mu:
			assert t.symbol in self.A and t.s_in in self.Q and t.s_out in self.Q
			entry = self._toEntry(t.symbol, t.s_in)
			if entry in self._mu: self._mu[entry].append(t)
			else: self._mu[entry] = [t]
		self.mu = mu # set{} # faire un dict sur le nom de l'état de départ concaténé au symbol
		self.currentState = I
		self.ctx = ctx

	def _toEntry(self, symbol, state):
		return str(symbol)+'-@->'+str(state.name)

	def next(self, symbol):
		s = self._mu[self._toEntry(symbol, self.currentState)]
		return s # { t.s_out for t in s}

	def do(self,t, symbol, msg):
		a = self.currentState.a_out(self.ctx, symbol, msg)
		b = t.action(self.ctx, symbol, msg)
		self.currentState = t.s_out
		c = self.currentState.a_in(self.ctx, symbol, msg)
		return (a, b, c)

	def applyDet(self, symbol, msg = None): #Suppose a deterministic FSM
		assert symbol in self.A, 'Unkown symbol'
		s = self.next(symbol)
		t = s[0]
		return self.do(t, symbol, msg)

	def end(self):
		return self.currentState in self.F

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
	print('main de FSM.py')
