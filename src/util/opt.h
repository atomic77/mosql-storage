#ifndef _OPT_H_
#define _OPT_H_

#ifdef __cplusplus
extern "C" {
#endif

enum optiontype {
	str_opt,
	int_opt,
	fla_opt,
};

struct option {
	char  flag;
	const char* desc;
	void* pvar;
	enum optiontype type;
};

int get_options(struct option* opt, int argc, char * const argv[]);
void get_options_string(struct option* opt, char* str);
void print_options(struct option* opt);

#ifdef __cplusplus
}
#endif

#endif
