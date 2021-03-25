SELECT DISTINCT B.F FROM Enrollments E, Boats B WHERE B.D = E.student_id AND E.course_id < 200 ORDER BY B.F, E.course_id;
