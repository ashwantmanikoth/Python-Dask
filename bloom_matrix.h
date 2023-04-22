#pragma GCC optimize("Ofast","unroll-loops","omit-frame-pointer","inline") //Optimization flags
//#pragma GCC option("arch=native","tune=native","no-zero-upper") //Enable AVX
//#pragma GCC target("avx")  //Enable AVX

#include <x86intrin.h> //AVX/SSE Extensions
#include <boost/python/numpy.hpp>
#include <boost/scoped_array.hpp>
#include <boost/python/stl_iterator.hpp>
#include <iostream>
#include <math.h>
#include <vector>
#include <string>
#include <bits/stdc++.h>
#include <thread>
#include<fstream>
#include <ctime>
#include <unistd.h>
#include <boost/tuple/tuple.hpp>
#include <cassert>
#include <memory> // std::shared_ptr
#include <stack>    // std::stack

#define NUM_THREADS 8
#define FILE_NAME "Bloom_Matrix"

using namespace std;
namespace p = boost::python;
namespace np = boost::python::numpy;

std::mutex myMutex;

/*g++ -std=c++0x test.cpp*/

double MERSENNES1[] = {pow(2,17) - 1, pow(2,31) - 1, pow(2,127) -1};
double MERSENNES2[] = {pow(2,19) - 1, pow(2,67) - 1, pow(2,257) -1};

#ifdef NUM_PART
const int num_part = NUM_PART;
#else
const int num_part = 100;
#endif


/// @brief Guard that will acquire the GIL upon construction, and
///        restore its state upon destruction.
class with_gil
{
public:
  with_gil()  { state_ = PyGILState_Ensure(); }
  ~with_gil() { PyGILState_Release(state_);   }

  with_gil(const with_gil&)            = delete;
  with_gil& operator=(const with_gil&) = delete;
private:
  PyGILState_STATE state_;
};

/// @brief Guard that will unlock the GIL upon construction, and
///        restore its staet upon destruction.
class without_gil
{
public:
  without_gil()  { state_ = PyEval_SaveThread(); }
  ~without_gil() { PyEval_RestoreThread(state_); }

  without_gil(const without_gil&)            = delete;
  without_gil& operator=(const without_gil&) = delete;
private:
  PyThreadState* state_;
};

/// @brief Guard that provides higher-level GIL controls.
class gil_guard
{
public:
  struct no_acquire_t {} // tag type used for gil acquire strategy
  static no_acquire;

  gil_guard()             { acquire(); }
  gil_guard(no_acquire_t) { release(); }
  ~gil_guard()            { while (!stack_.empty()) { restore(); } }

  void acquire()          { stack_.emplace(new with_gil); }
  void release()          { stack_.emplace(new without_gil); }
  void restore()          { stack_.pop(); }

  static bool owns_gil()
  {
    // For Python 3.4+, one can use `PyGILState_Check()`.
    return PyGILState_Check();
  }

  gil_guard(const gil_guard&)            = delete;
  gil_guard& operator=(const gil_guard&) = delete;

private:
  // Use std::shared_ptr<void> for type erasure.
  std::stack<std::shared_ptr<void>> stack_;
};



class BloomMatrix{
private:
	int filter_size, num_partitions, hash_count;
	bitset<num_part> *bm;
	vector<vector<int>> mutiple_part_list;

	double simple_hash(double val, double prime1, double prime2, double prime3) {
        //Compute a hash value from a list of integers and 3 primes
    	double result = fmod(((val + prime1) * prime2), prime3);
        return result;
    }


	double hash1(double val){
        //Basic hash function
        return simple_hash(val, MERSENNES1[0], MERSENNES1[1], MERSENNES1[2]);
    }


	double hash2(double val) {
        //Basic hash function #2
        return simple_hash(val, MERSENNES2[0], MERSENNES2[1], MERSENNES2[2]);
    }

public:
    BloomMatrix(int items_count, double fp_prob, int num_partitions);
    BloomMatrix(int filter_size, int hash_count, int num_partitions);

    ~BloomMatrix() {
    	//cout << endl << "BloomMatrix Destructor Called";
    	delete[] bm;
    	bm = NULL;
    	for (int i = 0 ; i < mutiple_part_list.size() ; i++)
    		mutiple_part_list[i].clear();
    	mutiple_part_list.clear();
    }

    double bloom_hash(double val, int seed);
    void add(np::ndarray const & array, int partition);
    void check_in_range(double values[], int low, int high, int *part_list);
    void check(int val, int pos, int *part_list);
    np::ndarray check_array(np::ndarray const & array);
    int getSize();
    void initBloomMatrix();
    p::tuple getParams() {
    	return p::make_tuple(filter_size, hash_count, num_partitions);
    }

    void print() {
    	for(int i = 0 ; i < filter_size ; i++) {
    		bitset<num_part> B = bm[i];
    		cout << B << endl;
    	}
    }

    /*
    thread memberThread(double values[], int low, int high, int *part_list){
    	return thread([=] {check(values, low, high, part_list);});
    }
    */

    string write_to_binary_file(char *rel_name);
    void read_from_binary_file(char *file_path, char* rel_name);

    p::list get_multiple_partition_info(){
    	p::list result;
    	vector<vector<int>>::iterator it;
        for (it = mutiple_part_list.begin(); it != mutiple_part_list.end(); ++it){
        	vector<int>::iterator it2;
        	vector<int> v2 = *it;
        	p::list inner;
        	for (it2 = v2.begin(); it2 != v2.end(); ++it2)
        		inner.append(*it2);
        	result.append(inner);
        }
        return result;
    	//return stl2py(mutiple_part_list);
    }


};

//int *part_list;
//vector<int> part_list;

BloomMatrix :: BloomMatrix(int items_count, double fp_prob, int num_partitions) {
	filter_size = int(round(-(items_count * log(fp_prob))/pow(log(2),2)));
    this->num_partitions = num_partitions;
    hash_count = int(round((filter_size/items_count) * log(2)));
    cout << "Size of Bloom Matrix : " << ((double)filter_size * num_partitions)/((double)8 * 1024 * 1024 *1024) << "GB" << endl;
    initBloomMatrix();
}

BloomMatrix :: BloomMatrix(int filter_size, int hash_count, int num_partitions) {
	this->filter_size = filter_size;
	this->hash_count = hash_count;
	this->num_partitions = num_partitions;
	//read_from_binary_file(file_path);
}

void BloomMatrix :: initBloomMatrix() {
	bm = new bitset<num_part>[filter_size];
	bitset<num_part> Bits;
	for (int i = 0 ; i < filter_size ; i++)
		bm[i] = Bits;
}

double BloomMatrix :: bloom_hash(double val, int seed) {
	double h1 = hash1(val);
	double h2 = hash2(val);
	double result = (h1 + seed * h2);
    return result;

}

void BloomMatrix :: add(np::ndarray const & array, int partition) {

    // Make sure we get doubles
    if (array.get_dtype() != np::dtype::get_builtin<double>()) {
        PyErr_SetString(PyExc_TypeError, "add::Incorrect array data type");
        p::throw_error_already_set();
    }

	size_t n = array.shape(0);
	double *values = reinterpret_cast<double*>(array.get_data());
	for(size_t i = 0 ; i < n ; i++) {
		int val = (int)values[i];
		//cout << endl << "Val = " << val ;
		for (int k = 1 ; k <= hash_count ; k++) {
			double h1 = bloom_hash(val, k);
			double h2 = fmod(h1,filter_size);
			int h = (int)h2;
			//cout << " " << h2;
			//dynamic_bitset <> set(bm[h]);
			//set[partition] = 1;
			//bm[h] = set;
			bm[h][partition] = 1;
		}
	}
}

void BloomMatrix ::check(int val, int pos, int *part_list) {

	//cout << endl << "check with value " << val << " called";
	bitset<num_part> result;
	result.set();
	//cout << "Result : " << result << endl;
	for (int k = 1 ; k <= hash_count ; k++) {
		double h1 = bloom_hash(val, k);
		double h2 = fmod(h1,filter_size);
		int h = (int)h2;
		bitset<num_part> set = bm[h];
		//cout << "Value : " << set << endl;
		result &= set;
		if (result.count() == 1) {
			//cout << endl << "Partition found after " << k << " hash functions";
			break;
		}
		//cout << "Result : " << result << endl;
	}
	vector<int>list;
	list.push_back(val);
	list.push_back(pos);
	for(int k = 0 ; k < num_partitions ; k++) {
		if(result.test(k))
			list.push_back(k);
	}
	if (list.size() < 3) {
		//cout << endl << "Element not found";
		part_list[pos] = -1;
	}
	else if (list.size() == 3)
		part_list[pos] = list[2];
	else {
		part_list[pos] = -1;
		std::lock_guard<std::mutex> myLock(myMutex);
		mutiple_part_list.push_back(list);
	}
}

void BloomMatrix :: check_in_range(double values[], int low, int high, int *part_list) {
	//cout << "check with range called " << low << " " << high << endl;
	for (int i = low ; i <= high ; i++) {
		int val = (int)values[i];
		check(val, i, part_list);
	}
	//cout << "check with range ended " << low << " " << high << endl;
}

//
np::ndarray BloomMatrix :: check_array(np::ndarray const & array) {

    // Make sure we get doubles
    if (array.get_dtype() != np::dtype::get_builtin<double>()) {
        PyErr_SetString(PyExc_TypeError, "check_array::Incorrect array data type");
        p::throw_error_already_set();
    }


	size_t num_values_to_check = array.shape(0);
    //cout << endl << "check_array called : " << num_values_to_check;

    double *values = reinterpret_cast<double*>(array.get_data());
	int *part_list = new int [num_values_to_check];

	PyGILState_STATE state = PyGILState_Ensure();
    //gil_guard gil(gil_guard::no_acquire);
    //assert(!gil.owns_gil());


	/*
	cout << "Values are : " << endl;

	for (size_t i = 0 ; i < num_values_to_check ; i++) {
		cout << endl << values[i];
	}
	*/


	vector<thread> v_threads;

	int num_elements_per_thread = num_values_to_check/NUM_THREADS;


	for(int i = 0 ; i < NUM_THREADS ; i++) {
		int low = i * num_elements_per_thread;
		int high = (i+1) * num_elements_per_thread - 1;

		if (i == NUM_THREADS - 1)
			high += num_values_to_check % NUM_THREADS;

		v_threads.push_back(std::thread(&BloomMatrix::check_in_range, this, values, low, high, part_list));
	}

	for_each(v_threads.begin(),v_threads.end(), mem_fn(&thread::join));

	//check_in_range(values, 0, num_values_to_check - 1, part_list);

	PyGILState_Release(state);
	  //gil.acquire();
	  //assert(gil.owns_gil());


	/*
	for (size_t i = 0 ; i < num_values_to_check ; i++) {
		cout << endl << values[i] << " : " << part_list[i];
	}
	*/

    // Turning the output into a numpy array
    np::dtype dt = np::dtype::get_builtin<int>();

    p::tuple shape = p::make_tuple(num_values_to_check); // It has shape (num_values_to_check,)
    p::tuple stride = p::make_tuple(sizeof(int)); // 1D array, so its just size of int

    np::ndarray result = np::from_data(part_list, dt, shape, stride, p::object());
    //cout << endl << "return to python from C++";
    return result;	//return py::make_tuple(result, mutiple_part_list);
}

int BloomMatrix :: getSize() {
	//return sizeof(bm);
	size_t size=0;
	for (int i = 0 ; i < filter_size ; i++) {
		bitset<num_part> set = bm[i];
		size += sizeof(set);
	}
	return size;
}


string  BloomMatrix :: write_to_binary_file(char *rel_name) {
	//write the Bloom matrix to a binary file
	//cout << endl << "Writing Bloom matrix to binary file";
	char file_name[200] = "";
	strcpy(file_name, FILE_NAME);
	strcat(file_name, "_");
	strcat(file_name, rel_name);
	strcat(file_name, ".bin");

#ifdef DEBUG
	cout << endl << "Writing to binary file : " << file_name;
#endif
	ofstream output(file_name,ios::binary);
	for (int i = 0 ; i < filter_size ; i++) {
		bitset<num_part> set = bm[i];
		unsigned long n = set.to_ulong();
		output.write( reinterpret_cast<const char*>(&n), sizeof(n) ) ;
	}
	output.close();
	return file_name;
}

void BloomMatrix :: read_from_binary_file(char *dir_path, char* rel_name) {
	//read a binary file and create a Bloom Matrix
	//cout << endl << "Reading Bloom matrix from binary file";
	bm = new bitset<num_part>[filter_size];
	char folder1[200] = "";
	strcpy(folder1, dir_path);
	strcat(folder1, FILE_NAME);
	strcat(folder1, "_");
	strcat(folder1, rel_name);
	char *file_path = strcat(folder1, ".bin");
#ifdef DEBUG
	cout << endl << "File path : " << file_path;
#endif
	ifstream input(file_path,ios::in | ios::binary);
	/*
	cout << endl << "Trying to open" << new_file_path;
	if(input.is_open())
		cout << endl << "Binary file is opened";
	else
		cout << endl << "Fail to open file";
		*/
	for (int i = 0 ; i < filter_size ; i++) {
	    unsigned long n ;
	    input.read( reinterpret_cast<char*>(&n), sizeof(n) ) ;
		bitset<num_part> set = n;
		bm[i] = set;
	}
	input.close();
}

