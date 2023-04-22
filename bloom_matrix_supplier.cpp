#include <boost/python.hpp>
#include <boost/python/numpy.hpp>
#include "bloom_matrix.h"
namespace p = boost::python;
namespace np = boost::python::numpy;


BOOST_PYTHON_MODULE(bloom_matrix_supplier) {

	Py_Initialize();
	PyEval_InitThreads();
	np::initialize();
	// bindings to Pet class
    p::class_<BloomMatrix>("BloomMatrix", p::init<int, double, int>())
		.def(p::init<int, int, int>())
        .def("add", &BloomMatrix::add)
        .def("check", &BloomMatrix::check_array)
		.def("write_to_binary_file", &BloomMatrix::write_to_binary_file)
		.def("read_from_binary_file", &BloomMatrix::read_from_binary_file)
		.def("get_multiple_partition_info", &BloomMatrix::get_multiple_partition_info)
		.def("getParams", &BloomMatrix::getParams)
    	.def("print", &BloomMatrix::print);
}
