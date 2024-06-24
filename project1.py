from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class proj1(MRJob):   
    def mapper(self, _, line):
        try:
            parts = line.split()
            region,ocean, country, city, month, day, year, temp_f = parts
            temp_f = float(temp_f)
            temp_c = (temp_f - 32) * 5.0 / 9.0
            yield (city, year), temp_c
            yield (city, 'all'), temp_c
                
        except Exception as e:
            self.increment_counter('Errors', 'MapperParseError', 1)
            self.stderr.write(f"Error parsing line: {line}\nException: {e}\n")
    
    def reducer_calculate_averages(self, key, values):
        try:
            temps = list(values)
            avg_temp = sum(temps) / len(temps)
            if key[1] == 'all':
                yield key[0], ('overall', avg_temp)
            else:
                yield key[0], (key[1], avg_temp)
        except Exception as e:
            self.increment_counter('Errors', 'ReducerCalcAverageError', 1)
            self.stderr.write(f"Error calculating averages for key: {key}\nException: {e}\n")
    
    def reducer_collect_city_data(self, key, values):
        try:
            overall_avg = None
            yearly_avgs = []
            for value in values:
                if value[0] == 'overall':
                    overall_avg = value[1]
                else:
                    yearly_avgs.append(value)
            results = []
            if overall_avg is not None:
                results = [(key, (year, avg_temp, overall_avg)) for year, avg_temp in yearly_avgs]
            else:
                self.increment_counter('Errors', 'MissingOverallAverage', 1)
                self.stderr.write(f"Missing overall average for city: {key}\n")
            
            sorted_results = sorted(results, key=lambda x: x[0])
            for result in sorted_results:
                yield result

        except Exception as e:
            self.increment_counter('Errors', 'ReducerCollectCityDataError', 1)
            self.stderr.write(f"Error collecting city data for key: {key}\nException: {e}\n")
    
    def reducer_find_anomalies(self, key, values):
        self.tau = float(jobconf_from_env('myjob.settings.tau'))
        try:
            threshold = self.tau
            anomalies = []
            for year, avg_temp, overall_avg in values:
                diff = avg_temp - overall_avg
                if diff > threshold:
                    anomalies.append((year,diff))
            anomalies_sorted = sorted(anomalies, key=lambda x: x[0], reverse=True)
            for values in anomalies_sorted:
                value = ','.join(str(x) for x in values)
                yield key,value
             
        except Exception as e:
            self.increment_counter('Errors', 'ReducerFindAnomaliesError', 1)
            self.stderr.write(f"Error finding anomalies for key: {key}\nException: {e}\n")
            
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_calculate_averages),
            MRStep(reducer=self.reducer_collect_city_data),
            MRStep(reducer=self.reducer_find_anomalies)
        ]

if __name__ == '__main__':
    proj1.run()
