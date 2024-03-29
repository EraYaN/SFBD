#include <Python.h>
#include <fstream>
#include "prefixes.h"
#include "timing.h"
#define GZSTREAM_NAMESPACE gz
#include "gzstream.h"
#define HEADER_MATCH "WARC-Target-URI:"
#define HEADER_MATCH_SIZE 16
#define STARTBUFFERSIZE 450000000
#define URLBUFFERSIZE 2049
#define PHONENUMBERBUFFERSIZE 64

static PyObject *
phonenumberfilter_load(PyObject *self, PyObject *args);
PyMODINIT_FUNC
PyInit_phonenumberfilter(void);
static PyObject * read_data(const char* filename, bool usecompression, bool deletefilewhendone);
static void process_data(char * data, PyObject* list, size_t size);

#pragma region Python stuff to setup module.
static PyObject *PhoneNumberFilterError;



static PyMethodDef PhoneNumberFilterMethods[] = {
    { "load",  phonenumberfilter_load, METH_VARARGS,
    "Load file and filter into phone numbers." },
    { NULL, NULL, 0, NULL }        /* Sentinel */
};

static struct PyModuleDef phonenumberfiltermodule = {
    PyModuleDef_HEAD_INIT,
    "phonenumberfilter",   /* name of module */
    NULL, /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module,
              or -1 if the module keeps state in global variables. */
    PhoneNumberFilterMethods
};

static PyObject *
phonenumberfilter_load(PyObject *self, PyObject *args)
{
    const char *file;
    int usecompression = 1;
    int deletefilewhendone = 0;

    if (!PyArg_ParseTuple(args, "s|pp", &file, &usecompression, &deletefilewhendone)) {
        PyErr_SetString(PhoneNumberFilterError, "Parsing arguments failed.");
        return NULL;
    }

    return read_data(file, usecompression != 0, deletefilewhendone != 0);

}

PyMODINIT_FUNC
PyInit_phonenumberfilter(void)
{
    PyObject *m;

    m = PyModule_Create(&phonenumberfiltermodule);
    if (m == NULL)
        return NULL;

    PhoneNumberFilterError = PyErr_NewException("phonenumberfilter.error", NULL, NULL);
    Py_INCREF(PhoneNumberFilterError);
    PyModule_AddObject(m, "error", PhoneNumberFilterError);
    return m;
}
#pragma endregion


char current_url[URLBUFFERSIZE];
bool has_url = false;
bool in_phonenumber = false;
char current_phone[PHONENUMBERBUFFERSIZE];
int phone_idx = 0;
int totalcounter = 0;
char newline = '\n';
char carriagereturn = '\r';
bool skippedzero = false;
bool seenothercharacter = false;
size_t endsize = 0;

static void inline write_to_output(PyObject* list) {
    current_phone[phone_idx] = '\0';
    PyObject *url = PyUnicode_FromString(current_url);
    PyObject *phone_number = PyUnicode_FromString(current_phone);
    PyObject *tuple = PyTuple_New(2);
    PyTuple_SetItem(tuple, 0, phone_number);
    PyTuple_SetItem(tuple, 1, url);
    PyList_Append(list, tuple);
}

static void inline reset_phonenumber() {
    in_phonenumber = false;
    phone_idx = 0;
    skippedzero = false;
}
static void inline reset_all() {
    has_url = false;
    reset_phonenumber();
    seenothercharacter = false;
}

static void inline add_to_phonenumber(char c) {
    current_phone[phone_idx] = c;
    phone_idx++;
}

static void inline write_to_output(std::ofstream* out) {
    current_phone[phone_idx] = '\0';
    *out << current_phone << std::endl;
}

static bool inline isprefix_3() {
    int key = (current_phone[1] - '0') | ((current_phone[2] - '0') << 4);
    return prefixes_3[key];
}
static bool inline isprefix_4() {
    int key = (current_phone[1] - '0') | ((current_phone[2] - '0') << 4) | ((current_phone[3] - '0') << 8);
    return prefixes_4[key];
}

static inline bool file_exists(const std::string& name) {
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

static void process_data(char * data, PyObject* out, size_t size) {

    endsize = size - HEADER_MATCH_SIZE;
    for (int i = 0; i < size; i++) {
        if (data[i] == 'W' && i < endsize) {
            if (memcmp(&data[i], HEADER_MATCH, HEADER_MATCH_SIZE) == 0) {
                i += HEADER_MATCH_SIZE + 1;
                int old_i = i;
                while (data[i] != newline && data[i] != carriagereturn && i < size) {
                    i++;
                }
                if (i - old_i < URLBUFFERSIZE-1) {
                    memcpy(current_url, &data[old_i], i - old_i);
                    current_url[i - old_i] = '\0';
                }
                else {
                    memcpy(current_url, &data[old_i], URLBUFFERSIZE-1);
                    current_url[URLBUFFERSIZE-1] = '\0';
                }
                has_url = true;
            }
            seenothercharacter = true;
        }
        if (has_url) {
            if ((data[i] >= '0' && data[i] <= '9') || data[i] == '-' || data[i] == '+' || data[i] == ' ' || data[i] == '(' || data[i] == ')') {
                //symbol or number
                if (in_phonenumber) {
                    if (phone_idx >= 11) {
                        reset_phonenumber();
                    }
                    else {
                        if (phone_idx == 4) {
                            if (!isprefix_3() && !isprefix_4()) {
                                reset_phonenumber();
                                continue;
                            }
                        }
                        if (data[i] >= '0' && data[i] <= '9') {
                            //number
                            if (phone_idx == 1 && data[i] == '0' && !skippedzero) {
                                //bad skip
                                skippedzero = true;
                            }
                            else if (phone_idx == 1 && data[i] == '0' && skippedzero) {
                                //bad end
                                reset_phonenumber();
                            }
                            else {
                                add_to_phonenumber(data[i]);
                            }
                        }
                    }
                }
                else if (seenothercharacter) {
                    if (data[i] == '0') {
                        if (data[i + 1] == '0' && data[i + 2] == '3'&&data[i + 3] == '1') {
                            in_phonenumber = true;
                            add_to_phonenumber('0');
                            i += 3;
                        }
                        /*else if ((data[i + 1] == '8' || data[i + 1] == '9') && data[i + 2] == '0'&&data[i + 3] == '0') {
                            in_phonenumber = true;
                            add_to_phonenumber('0');
                            add_to_phonenumber(data[i + 1]);
                            add_to_phonenumber('0');
                            add_to_phonenumber('0');
                            i += 3;
                        }*/
                    }
                    else if (data[i] == '+') {
                        if ((data[i + 1] == '3'&&data[i + 2] == '1')) {
                            in_phonenumber = true;
                            add_to_phonenumber('0');
                            i += 2;
                        }
                        else if (data[i + 2] == '3'&&data[i + 3] == '1') {
                            in_phonenumber = true;
                            add_to_phonenumber('0');
                            i += 3;
                        }
                        else if (data[i + 3] == '3'&&data[i + 4] == '1') {
                            in_phonenumber = true;
                            add_to_phonenumber('0');
                            i += 4;
                        }
                    }
                }
                else {
                    //we are in a huge string of numbers.
                }
            }
            else {
                if (in_phonenumber) {
                    if (phone_idx == 8 || phone_idx == 11) {
                        if ((current_phone[1] == '8' || current_phone[1] == '9') && current_phone[2] == '0' && current_phone[3] == '0') {
                            //0800/0900 number
                            write_to_output(out);
                        }
                    }
                    else if (phone_idx == 10) {
                        //normal number
                        write_to_output(out);
                    }
                }
                reset_phonenumber();
                seenothercharacter = true;
            }
        }
    }
}
static PyObject * read_data(const char* filename, bool usecompression, bool deletefilewhendone) {
    /*if(usecompression)
        std::cout << "Processing compressed file: " << filename << std::endl;
    else
        std::cout << "Processing file: " << filename << std::endl;*/
    perftime_t t0, t1, t2, t3;
    t0 = now();
    if (!file_exists(filename)) {
        char errorbuf[1024];
        snprintf(errorbuf, 1024, "File %s does not exist.", filename);
        PyErr_SetString(PhoneNumberFilterError, errorbuf);
        return NULL;
    }
    std::istream * is;
    if (usecompression) {
        is = new gz::igzstream(filename);
    }
    else {
        is = new std::ifstream(filename);
    }

    if (is) {
        // Determine the file length
        char* buffer = new char[STARTBUFFERSIZE];
        // Load the data
        is->read(buffer, STARTBUFFERSIZE);
        size_t size = is->gcount();
        t1 = now();
        reset_all();
        PyObject *list = PyList_New(0);
        process_data(buffer, list, size);
        t2 = now();
        delete[] buffer;
        delete is;

        if (deletefilewhendone) {
            if (remove(filename) != 0) {
                //std::cout << "Could not remove file " << filename << " after processing." << std::endl;
            }
            else {
                //std::cout << "Removed file " << filename << " after processing." << std::endl;
            }
        }
        else {
            //std::cout << "Leaving file " << filename << " after processing." << std::endl;
        }
        t3 = now();
        return PyTuple_Pack(4, list, PyFloat_FromDouble(diffToNanoseconds(t0, t1)), PyFloat_FromDouble(diffToNanoseconds(t1, t2)), PyFloat_FromDouble(diffToNanoseconds(t2, t3)));;
    }
    else {
        char errorbuf[1024];
        snprintf(errorbuf, 1024, "Could not open file: %s.", filename);
        PyErr_SetString(PhoneNumberFilterError, errorbuf);
        return NULL;
    }
}