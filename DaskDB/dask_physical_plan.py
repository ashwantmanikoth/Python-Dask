class PhysicalPlan:
    
    id_to_table_mappings = {}
    mt_count = 0;
    
    def init(self, query_plan):
        dask_plan = []
        for plan in query_plan:
            dask_plan.append(self.init_plan(plan))
        return dask_plan

    def init_plan(self, plan):
        mpp = plan
        dask_plan = {}
        dask_plan['operators'] = {}
        if 'plans' in mpp['plan']:
            physical_plan = mpp['plan']['plans'][0]['fragments']
        else:
            physical_plan = mpp['plan']['fragments']
        level = len(physical_plan)
        for i in physical_plan:
            dask_plan['operators'][level] = []
            for j in i['operators']:
                dask_plan['operators'][level].append(self.map_to_dask_physical(j))
            level-=1
        self.add_dependencies(dask_plan)
        return dask_plan
    
    def add_dependencies(self, dask_plan):
        for key,value in sorted(dask_plan['operators'].items()):
            for i in range(len(value)):
                #print(dask_plan['operators'][key][i])
                if 'argChild' in dask_plan['operators'][key][i]:
                    #look for id 
                    look_id = dask_plan['operators'][key][i]['argChild']
                    dask_plan['operators'][key][i]['data_table'] = self.id_to_table_mappings[look_id]
                    self.id_to_table_mappings[dask_plan['operators'][key][i]['id']] = self.id_to_table_mappings[look_id]
                j=1
                while ('argChild' + str(j)) in dask_plan['operators'][key][i]:
                    look_id = dask_plan['operators'][key][i]['argChild' +  str(j)]
                    dask_plan['operators'][key][i]['data_table' + str(j)] = self.id_to_table_mappings[int(look_id)]
                    self.id_to_table_mappings[dask_plan['operators'][key][i]['id']] = self.id_to_table_mappings[int(look_id)]
                    dask_plan['operators'][key][i]['data_table'] = 'merged_table_' + self.id_to_table_mappings[int(look_id)]
                    self.id_to_table_mappings[dask_plan['operators'][key][i]['id']] = 'merged_table_' + self.id_to_table_mappings[int(look_id)]
                    j+=1

        
    
    def map_to_dask_physical(self, myria_task):
        dask_dict = dict()
        dask_dict['id']=myria_task['opId']
       
        if 'argChild' in myria_task:
            dask_dict['argChild'] = myria_task['argChild']
        i=1
        while ('argChild' + str(i)) in myria_task:
            dask_dict['argChild'+ str(i)] = myria_task['argChild'+ str(i)]
            i+=1
        if 'MyriaScan' in myria_task['opName']:
            dask_dict['opType']=myria_task['opType']
            dask_dict['operation'] = 'read_csv'
            dask_dict['table_name'] = myria_task['relationKey']['relationName']
            dask_dict['table_name_temp'] = '_temp_' + myria_task['relationKey']['relationName']
            self.id_to_table_mappings[myria_task['opId']] = dask_dict['table_name']
        elif 'MyriaSelect' in myria_task['opName']:
            dask_dict['operation'] = myria_task['opType']
            if 'left' in myria_task['argPredicate']['rootExpressionOperator']:
                dask_dict['left'] = myria_task['argPredicate']['rootExpressionOperator']['left']
                dask_dict['right'] = myria_task['argPredicate']['rootExpressionOperator']['right']
                dask_dict['type'] = myria_task['argPredicate']['rootExpressionOperator']['type']
                dask_dict['isNot'] = False
            else:
                dask_dict['left'] = myria_task['argPredicate']['rootExpressionOperator']['operand']['left']
                dask_dict['right'] = myria_task['argPredicate']['rootExpressionOperator']['operand']['right']
                dask_dict['type'] = myria_task['argPredicate']['rootExpressionOperator']['operand']['type']
                dask_dict['isNot'] = True
        elif 'MyriaApply' in myria_task['opName']:
            if 'PYUDF' in myria_task['opName']:
                dask_dict['operation'] = 'PytyhonUDF'
                dask_dict['returned'] = myria_task['emitExpressions'][0]['outputName']
                dask_dict['parameters'] = myria_task['emitExpressions'][0]['rootExpressionOperator']['children']
                dask_dict['function_name'] = myria_task['emitExpressions'][0]['rootExpressionOperator']['name']
            else:
                dask_dict['operation'] = 'Column_Mapping'
                dask_dict['rename_columns'] = []
                dask_dict['new_columns'] = {} 
                for i in myria_task['emitExpressions']:
                    if 'columnIdx' in i['rootExpressionOperator']:
                        dask_dict['rename_columns'].append((i['rootExpressionOperator']['columnIdx'],i['outputName']))
                    else:
                        dask_dict['new_columns'][i['outputName']] = i['rootExpressionOperator']
        elif 'MyriaStoreTemp' in myria_task['opName']: 
            dask_dict['operation'] = 'StoreTempResult'
            dask_dict['variable'] = 'user_input'
        elif 'MyriaSymmetricHashJoin' in myria_task['opName']: 
            self.mt_count += 1
            dask_dict['operation'] = 'HashJoin'
            dask_dict['operation_type'] = 'Merge'
            dask_dict['join_col_1'] = myria_task['argColumns1']
            dask_dict['join_col_2'] = myria_task['argColumns2']
            dask_dict['select_col_1'] = myria_task['argSelect1'] 
            dask_dict['select_col_2'] = myria_task['argSelect2']
        elif 'MyriaShuffleConsumer' in myria_task['opName']: 
            dask_dict['operation'] = 'get_child_data'
            dask_dict['operation_type'] = 'Shuffle'
            dask_dict['argChild'] =  myria_task['argOperatorId']
        elif 'MyriaShuffleProducer' in myria_task['opName']: 
            dask_dict['operation'] = 'save_temp_data'
            dask_dict['operation_type'] = 'Shuffle'
            dask_dict['temp_table_name'] =  'temp'
        elif 'MyriaSplitConsumer' in myria_task['opName']: 
            dask_dict['operation'] = 'get_child_data'
            dask_dict['operation_type'] = 'Split'
            dask_dict['argChild'] =  myria_task['argOperatorId']
        elif 'MyriaSplitProducer' in myria_task['opName']: 
            dask_dict['operation'] = 'save_temp_data'
            dask_dict['operation_type'] = 'Split'
            dask_dict['temp_table_name'] =  'temp'
        elif 'MyriaCollectConsumer' in myria_task['opName']: 
            dask_dict['operation'] = 'get_child_data'
            dask_dict['operation_type'] = 'Collect'
            dask_dict['temp_table_name'] =  'temp'
            dask_dict['argChild'] =  myria_task['argOperatorId']
        elif 'MyriaCollectProducer' in myria_task['opName']: 
            dask_dict['operation'] = 'save_temp_data'
            dask_dict['operation_type'] = 'Collect'
            dask_dict['temp_table_name'] =  'temp'
        elif 'MyriaStore' in myria_task['opName']: 
            dask_dict['operation'] = 'final_output'
            dask_dict['operation_type'] = 'compute'
        elif 'MyriaDupElim' in myria_task['opName']: 
            dask_dict['operation'] = myria_task['opName']
            dask_dict['operation_type'] = myria_task['opType']
        elif 'MyriaGroupBy' in myria_task['opName']:
                dask_dict['operation'] = 'Groupby'
                dask_dict['operation_type'] = myria_task['opType']
                dask_dict['arguments'] = myria_task['aggregators']
                dask_dict['grouby_fields'] = myria_task['argGroupFields']
        elif 'MyriaInMemoryOrderBy' in myria_task['opName']:
            dask_dict['operation'] = 'Orderby'
            dask_dict['SortColumn'] = myria_task['argSortColumns']
            dask_dict['ascending'] = myria_task['argAscending']
        elif 'MyriaLimit' in myria_task['opName']:
            dask_dict['operation'] = 'Limit'
            dask_dict['num_of_rows'] = myria_task['numTuples']   
        else:
            dask_dict['operation'] = myria_task['opName']
        return dask_dict