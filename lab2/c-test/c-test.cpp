#include <iostream>
#include <fstream>
#include "timing.h"
#include <string>
#include <memory>
#include <sys/stat.h>
#include "prefixes.h"
#define GZSTREAM_NAMESPACE gz
#include "gzstream.h"

#define HEADER_MATCH "WARC-Target-URI:"
#define HEADER_MATCH_SIZE 16
#define STARTBUFFERSIZE 400000000
#define URLBUFFERSIZE 8192
#define PHONENUMBERBUFFERSIZE 64

char current_url[URLBUFFERSIZE];
bool has_url = false;
bool in_phonenumber = false;
char current_phone[PHONENUMBERBUFFERSIZE];
int phone_idx = 0;
int totalcounter = 0;
char newline = '\n';
//bool startedwithone = false;
//bool specialshortnumber = false;
bool skippedzero = false;
bool seenothercharacter = false;
size_t endsize = 0;

static void inline reset_phonenumber() {
    in_phonenumber = false;
    phone_idx = 0;
    //startedwithone = false;
    //specialshortnumber = false;
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
    *out << current_phone << std::endl; // << ", " << current_url << std::endl;
    totalcounter++;
}

static bool inline isprefix_3() {
    int key = (current_phone[1]-'0') | ((current_phone[2]-'0')<<4);
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

static void process_data(char * data, std::ofstream* out, size_t size) {

    endsize = size - HEADER_MATCH_SIZE;
    for (int i = 0; i < size; i++) {
        if (data[i] == 'W' && i < endsize) {
            if (memcmp(&data[i], HEADER_MATCH, HEADER_MATCH_SIZE) == 0) {
                i += HEADER_MATCH_SIZE + 1;
                int old_i = i;
                while (data[i] != newline && data[i] != carriagereturn) {
                    i++;
                }
                memcpy(current_url, &data[old_i], i - old_i);
                current_url[i - old_i] = '\0';
                has_url = true;
                //std::cout << "New URL: " << current_url << std::endl;
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
                        add_to_phonenumber(data[i]);
                        in_phonenumber = true;
                    }
                    /*else if (data[i] == '1') { //for 112 and 14xx(xx) 16xx 18xx numbers
                        add_to_phonenumber(data[i]);
                        in_phonenumber = true;
                        startedwithone = true;
                        if ((data[i + 1] == '1'&&data[i + 2] == '2')) {
                            add_to_phonenumber('1');
                            add_to_phonenumber('2');
                            i += 2;
                            write_to_output(out);
                            reset_phonenumber();
                        }
                        else if ((data[i + 1] == '4'&&data[i + 2] == '4')) {
                            add_to_phonenumber('4');
                            add_to_phonenumber('4');
                            i += 2;
                            write_to_output(out);
                            reset_phonenumber();
                        }
                        else if ((data[i + 1] == '4') && data[i + 2] == '0')
                        {
                            add_to_phonenumber('4');
                            add_to_phonenumber('0');
                            i += 2;
                            specialshortnumber = true;
                        }
                        else if ((data[i + 1] == '1') && data[i + 2] == '6')
                        {
                            add_to_phonenumber('1');
                            add_to_phonenumber('6');
                            i += 2;
                            specialshortnumber = true;
                        }
                        else if (data[i + 1] == '6' || data[i + 1] == '8') {
                            add_to_phonenumber(data[i + 1]);
                            i += 1;
                            specialshortnumber = true;
                        }
                        else {
                            reset_phonenumber();
                        }
                    }*/
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
                    /*else if (phone_idx >= 4 && phone_idx <= 6) {
                        if (specialshortnumber) {
                            //subscriberservices, municipalities, carrierselect etc
                            write_to_output(out);
                        }
                    }*/
                }
                reset_phonenumber();
                seenothercharacter = true;
            }
        }
    }
}

int main() {

    //const char* segmentfile = "C:\\commoncrawl\\SBD-Lab2\\input\\custom_test-uncompressed.txt";
    const char* segmentfile = "C:\\commoncrawl\\SBD-Lab2\\input\\test_wet_16.txt";
    const char* outfilename = "C:\\commoncrawl\\SBD-Lab2\\output_c-test.txt";
    double inittime = 0;
    double readingtime = 0;
    double processingtime = 0;
    double cleanuptime = 0;
    size_t total_size = 0;
    if (!file_exists(segmentfile)) {
        std::cerr << "Segment file " << segmentfile << " does not exist." << std::endl;
        std::cin.get();
        return 1;
    }
    std::cout << "Starting..." << std::endl;
    std::ofstream outfile(outfilename);
    std::ifstream file(segmentfile);
    std::string filename;
    if (file.is_open()) {
       /* const std::streamsize buff_size = 1 << 23;
        char * buff = new char[buff_size];*/
        while (std::getline(file, filename))
        {
            size_t index = filename.find(':');
            if (index == std::string::npos)
                continue;
            else
                index++;
            std::cout << "Processing file " << filename.substr(index) << std::endl;
            // Process str   
            perftime_t t0, t1, t2, t3, t4;
            if (!file_exists(filename.substr(index))) {
                std::cerr << "File " << filename.substr(index)<< " does not exist." << std::endl;
                continue;
            }
            t0 = now();
            
            //std::unique_ptr<std::istream> is = std::unique_ptr< std::istream >(new zstr::ifstream(filename.substr(index)));
            gz::igzstream is(filename.substr(index).c_str());
            //is->exceptions(std::ios_base::badbit);
            if (is.good())
            {
                // Determine the file length
                //is.seekg(0, std::ios_base::end);
               // size_t size = is.tellg();
                //is.seekg(0, std::ios_base::beg);
                
                char* buffer = new char[STARTBUFFERSIZE];
                t1 = now();
                // Load the data
                is.read(buffer, STARTBUFFERSIZE);
                size_t size = is.gcount();
                std::cout << "File size: " << size << std::endl;
                t2 = now();

                reset_all();
                process_data(buffer, &outfile, size);

                t3 = now();

                delete[] buffer;

                t4  = now();
                /*t1 = t2 = now();
                size_t size =0 ;
                
                while (true)
                {
                    is.read(buff, buff_size);
                    std::streamsize cnt = is.gcount();
                    if (cnt == 0) break;

                    process_data(buff, &outfile, cnt);
                    size += cnt;
                }
                std::cout << "File size: " << size << std::endl;
                t3 = now();
                t4 = now();*/

                inittime += diffToNanoseconds(t0, t1);
                readingtime += diffToNanoseconds(t1, t2);

                processingtime += diffToNanoseconds(t2, t3);

                cleanuptime += diffToNanoseconds(t3, t4);
                total_size += size;
            }
            else {
                std::cerr << "File " << filename.substr(index).c_str() << " could not be opened." << std::endl;
            }
        }

        outfile.close();

        std::cout << "Found " << totalcounter << " non unique phonenumbers." << std::endl;

        double processingbandwidth = (total_size / 1e9) / (processingtime / 1e9);
        double readingbandwidth = (total_size / 1e9) / (readingtime / 1e9);

        std::cout << "Init time: " << (inittime / 1e6) << " ms" << std::endl;
        std::cout << "Reading time: " << (readingtime / 1e6) << " ms (" << readingbandwidth << " GB/s)" << std::endl;
        std::cout << "Processing Time: " << (processingtime / 1e6) << " ms (" << processingbandwidth << " GB/s)" << std::endl;
        std::cout << "Cleanup Time: " << (cleanuptime / 1e6) << " ms" << std::endl;
        std::cout << "Total Size: " << (total_size / 1e9) << " GB" << std::endl;
        std::cout << "Total Time: " << ((inittime+ readingtime+ processingtime+ cleanuptime) / 1e9) << " s" << std::endl;
        std::cout << "Done" << std::endl;
        //delete[] buff;
    }
    else {
        std::cerr << "Could not open segment file." << std::endl;
    }    
    std::cin.get();


    return 0;
}