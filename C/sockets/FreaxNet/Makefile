FreaxNet: main.o connect.o cmd.o
	gcc $^ -o $@ 
    main.o:	cmd.h connect.h
    cmd.o:	cmd.h
    connect.o:	connect.h

install: FreaxNet
	cp FreaxNet $(DESTDIR)/usr/bin
	cp FreaxNet.1.gz $(DESTDIR)/usr/share/man/man1

uninstall:
	rm -f $(DESTDIR)/usr/bin/FreaxNet
	rm -f $(DESTDIR)/usr/share/man/man1/FreaxNet.1.gz

clean:
	rm -f FreaxNet
	rm -f *.o

