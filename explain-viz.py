# This Python scripts loads a MongoDB explain plan *with executionStats* and attempts to draw
# a graphical representation of the executed work. The script will attempt to load the explain
# plan from a file called 'explain.json'. This can be overriden using the first command line argument
# to the script.

import graphviz
import json
import sys


def read_explain_plan(filename):
    """
    Attempts to read a file from the given path as JSON as return the document as a dictionary
    :param filename: The JSON filename to be read
    :return: The read JSON file as a dictionary
    """
    file = open(filename, )
    # returns JSON object as a dictionary
    return json.load(file)


def generate_graph(explain, out_file):
    """
    Generates a graph from the supplied explain plan.
    :param explain: The explain to be analysed.
    :param out_file: The output graphviz file
    """
    dot = graphviz.Digraph(comment='Explain')
    dot.attr('node', shape='box')

    command = "unknown"
    sharded = False

    node_index = 0

    if 'command' in explain.keys():
        if 'find' in explain['command'].keys():
            command = "find"
            print("This is a find")
            if 'shards' in explain['queryPlanner']['winningPlan'].keys():
                sharded = True
        elif 'aggregate' in explain['command'].keys():
            command = "aggregate"
            print("This is an aggregate")
            if 'shards' in explain.keys():
                sharded = True
        else:
            print("Unsupported command")
            exit()
    else:
        # try to determine type of operation in a different way
        # not sure if this is required, but had some sample files that required it
        if 'shards' in explain.keys():
            sharded = True
        if 'splitPipeline' in explain.keys():
            command = "aggregate"

    node_index = node_index + 1
    dot.attr('node', shape='doublecircle')
    dot.node(str(node_index), "")
    dot.attr('node', shape='box')

    if command == 'find':
        print("sharded find todo")
        node_index = visit_execution_stats_node(dot, explain['executionStats'],
                                                'executionStages', node_index, node_index)
    elif command == 'aggregate' and sharded is True:
        print("sharded aggregate todo")

        if 'mergeType' in list(explain.keys()):
            node_index = node_index + 1
            dot.node(str(node_index), "mergeType: " + explain['mergeType'])
            dot.edge(str(node_index), str(node_index - 1))

        root_index = node_index
        for shardKey in list(explain['shards'].keys()):
            node_index = node_index + 1
            dot.attr('node', shape='egg')
            dot.node(str(node_index), shardKey)
            dot.attr('node', shape='box')
            dot.edge(str(node_index), str(root_index))
            parent_index = node_index

            shard = explain['shards'][shardKey]

            if 'stages' in list(shard.keys()):
                node_index = visit_aggregate_stages(dot, shard['stages'], parent_index, node_index)
            elif 'executionStats' in list(shard.keys()):
                node_index = visit_execution_stats_node(dot, shard['executionStats'],
                                                        'executionStages', node_index, node_index)
    elif command == 'aggregate' and sharded is False:
        parent_index = node_index
        if 'stages' in list(explain.keys()):
            visit_aggregate_stages(dot, explain['stages'], parent_index, node_index)
        elif 'executionStats' in list(explain.keys()):
            node_index = visit_execution_stats_node(dot, explain['executionStats'],
                                                    'executionStages', node_index, node_index)

    dot.render(out_file, view=True)


def template_node(name, node):
    n_returned = None
    if 'nReturned' in node.keys():
        n_returned = node['nReturned']

    docs_examined = None
    if 'docsExamined' in node.keys():
        docs_examined = node['docsExamined']

    keys_examined = None
    if 'keysExamined' in node.keys():
        keys_examined = node['keysExamined']

    index_name = None
    if 'indexName' in node.keys():
        index_name = node['indexName']

    return add_node(name,
                    n_returned,
                    docs_examined,
                    keys_examined,
                    index_name
                    )


def add_node(name,
             n_returned=None,
             docs_examined=None,
             keys_examined=None,
             index_name=None):
    ret = '''<
    <table border="0" cellborder="0" cellspacing="1">
         <tr><td align="left"><b>''' + name + '''</b></td></tr>'''

    if n_returned is not None:
        ret = ret + '<tr><td align="left"><font color="darkgreen">nReturned: ' + str(n_returned) + '</font></td></tr>'

    if docs_examined is not None:
        ret = ret + '<tr><td align="left"><font color="blue">docsExamined: ' + str(docs_examined) + '</font></td></tr>'

    if keys_examined is not None:
        ret = ret + '<tr><td align="left"><font color="red">keysExamined: ' + str(keys_examined) + '</font></td></tr>'

    if index_name is not None:
        ret = ret + '<tr><td align="left"><font color="orange">indexName: ' + str(index_name) + '</font></td></tr>'

    ret = ret + '''</table>>'''
    return ret


def visit_aggregate_stages(dot_graph, stages, parent_index, node_index):
    for stage in reversed(stages):
        node_index = visit_aggregate_stage(dot_graph, stage, parent_index, node_index)
        parent_index = node_index
    return node_index


def visit_aggregate_stage(dot_graph, stage, parent_name, node_index):
    stage_name = find_aggregate_stage_key_name(stage)
    current_node = stage[stage_name]

    sub_graph = graphviz.Digraph(name="cluster_" + str(parent_name), node_attr={'shape': 'box'})

    print("agg stage: " + stage_name)

    if 'executionStats' in list(current_node.keys()):
        node_index = node_index + 1
        sub_graph.node(str(node_index), template_node(stage_name, current_node))
        sub_graph.edge(str(node_index), str(parent_name),
                       # "nReturned: " + str(current_node['nReturned'])
                       # + ",totalKeysExamined:" +  str(execuction_stats['totalKeysExamined'])
                       # + ",totalDocsExamined:" + str(execuction_stats['totalDocsExamined'])
                       )

        sub_graph.attr(label=stage_name)
        node_index = visit_execution_stats_node(sub_graph, current_node['executionStats'],
                                                'executionStages', node_index, node_index)
        dot_graph.subgraph(sub_graph)
    else:
        node_index = node_index + 1
        sub_graph.node(str(node_index), template_node(stage_name, current_node))
        sub_graph.attr(label=stage_name)
        dot_graph.subgraph(sub_graph)
        dot_graph.edge(str(node_index), str(parent_name))

    return node_index


def find_aggregate_stage_key_name(stage):
    for key in list(stage.keys()):
        if key.startswith("$"):
            return key

    return None


def visit_execution_stats_node(dotGraph, execuction_stats, stageFieldName, parentName, node_index):
    node_index = node_index + 1

    current_node = execuction_stats
    if stageFieldName == 'executionStages':
        current_node = execuction_stats[stageFieldName]
        print("currnode: " + current_node['stage'])
    else:
        print("currnode: " + current_node['stage'])

    # dotGraph.node(str(node_index), current_node['stage'])
    dotGraph.node(str(node_index), template_node(current_node['stage'], current_node))

    dotGraph.edge(str(node_index), str(parentName),
                  "nReturned: " + str(current_node['nReturned'])
                  # + ",totalKeysExamined:" +  str(execuction_stats['totalKeysExamined'])
                  # + ",totalDocsExamined:" + str(execuction_stats['totalDocsExamined'])
                  )

    if 'inputStage' in current_node.keys():
        print(" --- " + current_node['inputStage']['stage'])
        return visit_execution_stats_node(dotGraph, current_node['inputStage'],
                                          'inputStage', node_index, node_index)
    elif 'inputStages' in current_node.keys():
        new_parent = node_index
        for stage in current_node['inputStages']:
            node_index = visit_execution_stats_node(dotGraph, stage,
                                                    'inputStage', new_parent, node_index)
        return node_index
    elif 'shards' in current_node.keys():
        shards_parent = node_index
        for shard in list(current_node['shards']):
            print(shard)
            node_index = node_index + 1
            dotGraph.attr('node', shape='egg')
            dotGraph.node(str(node_index), shard['shardName'])
            dotGraph.edge(str(node_index), str(shards_parent),
                          "nReturned: " + str(shard['nReturned'])
                          # + ",totalKeysExamined:" +  str(execuction_stats['totalKeysExamined'])
                          # + ",totalDocsExamined:" + str(execuction_stats['totalDocsExamined'])
                          )
            dotGraph.attr('node', shape='box')
            node_index = visit_execution_stats_node(dotGraph, shard,
                                                    'executionStages', node_index, node_index)
    else:
        return node_index


def agg_pipeline_node(dotGraph, stages, parentName, node_index):
    for stage in stages:
        node_index = node_index + 1
        print(node_index)
        print("x")
        print(stage)
        keys = list(stage.keys())
        print(keys[0])
        dotGraph.node(str(node_index), keys[0])
        if parentName is not None:
            dotGraph.edge(str(parentName), str(node_index), "nReturned: " + str(stage['nReturned']))
    return node_index


def visitnode(dotGraph, stages, parentName, node_index):
    print(stages)
    for stage in stages:
        node_index = node_index + 1
        print(node_index)
        print("x")
        print(stage)
        keys = list(stage.keys())
        print(keys[0])
        dotGraph.node(str(node_index), keys[0])
        if parentName is not None:
            dotGraph.edge(str(parentName), str(node_index), "nReturned: " + str(stage['nReturned']))
    return node_index


def start(filename):
    """
    Starts the process against the given filename
    :param filename: The filename to be processed
    """
    explain = read_explain_plan(filename)
    generate_graph(explain, "output/" + filename + ".gv")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    f = "explain.json"

    if len(sys.argv) >= 2:
        f = sys.argv[1]

    start(f)
