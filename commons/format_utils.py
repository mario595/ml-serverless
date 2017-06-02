import logging
import json

def format_queue(queue):
	i = 1
	text = 'Here is the current status of the queue:\n'
	for item in queue:
		if i == 1:
			text += "*%d. %s (%s)*\n" % (i, item['username'], item['branch'])
		else:
			text += "%d. %s (%s)\n" % (i, item['username'], item['branch'])
		i+= 1

	if i == 1:
		text = 'The queue is currently empty!'
	return text