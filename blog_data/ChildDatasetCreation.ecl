#WORKUNIT('name', 'Sample Data Creation');

CreateData(numAttributes, numRecords, numChildAttributes = 0, numChildRecords = 0) := FUNCTIONMACRO
    #DECLARE(x);
    #UNIQUENAME(finalResults);

    #UNIQUENAME(RecLayout);
    LOCAL %RecLayout% := RECORD
        #SET(x, 0)
        #LOOP
            #IF(%x% < numAttributes)
                UNSIGNED4  #EXPAND('field' + (STRING)(%x% + 1));
                #SET(x, %x% + 1)
            #ELSE
                #BREAK
            #END
        #END
    END;

    #UNIQUENAME(parentData);
    LOCAL %parentData% := DATASET
        (
            numRecords,
            TRANSFORM
                (
                    %RecLayout%,
                    #SET(x, 0)
                    #LOOP
                        #IF(%x% < numAttributes)
                            #IF(%x% > 0) , #END
                            SELF.#EXPAND('field' + (STRING)(%x% + 1)) := RANDOM()
                            #SET(x, %x% + 1)
                        #ELSE
                            #BREAK
                        #END
                    #END
                )
        );

    #IF(numChildAttributes > 0 AND numChildRecords > 0)
        #UNIQUENAME(ChildRecLayout);
        LOCAL %ChildRecLayout% := RECORD
            #SET(x, 0)
            #LOOP
                #IF(%x% < numChildAttributes)
                    UNSIGNED4  #EXPAND('child' + (STRING)(%x% + 1));
                    #SET(x, %x% + 1)
                #ELSE
                    #BREAK
                #END
            #END
        END;

        #UNIQUENAME(childData);
        LOCAL %childData% := DATASET
            (
                numChildRecords,
                TRANSFORM
                    (
                        %ChildRecLayout%,
                        #SET(x, 0)
                        #LOOP
                            #IF(%x% < numChildAttributes)
                                #IF(%x% > 0) , #END
                                SELF.#EXPAND('child' + (STRING)(%x% + 1)) := RANDOM()
                                #SET(x, %x% + 1)
                            #ELSE
                                #BREAK
                            #END
                        #END
                    )
            );
        
        #UNIQUENAME(MergedLayout);
        %MergedLayout% := RECORD
            %RecLayout%;
            DATASET(%ChildRecLayout%)   children;
        END;

        %finalResults% := PROJECT
            (
                %parentData%,
                TRANSFORM
                    (
                        %MergedLayout%,
                        SELF.children := %childData%,
                        SELF := LEFT
                    )
            );
    #ELSE
        %finalResults% := %parentData%;
    #END
    
    RETURN DISTRIBUTE(%finalResults%);
ENDMACRO;

// CreateData(numAttributes, numRecords, numChildAttributes = 0, numChildRecords = 0)

// ds := CreateData(5, 10);
ds := CreateData(5, 10, 3, 4);
OUTPUT(ds, ,'~fuse_testing::childds1');
