using TaskLocalValues
export gpufuse


struct FusionTaskQueue <: AbstractTaskQueue
    running_tasks::Vector{WeakRef}
    FusionTaskQueue() = new(WeakRef[])
end


function Dagger.enqueue!(queue::FusionTaskQueue, spec::Pair{Dagger.DTaskSpec, DTask})
     println("Inside Enqueue", spec[2])
     push!(queue.running_tasks, WeakRef(spec))    
end

function gpufuse(f)
    queue = FusionTaskQueue()
    with_options(f; task_queue = queue) 
    dofusion(queue)
    # upper = get_options(:task_queue, EagerTaskQueue())
    # enqueue!(upper, queue.running_tasks)
    # println(queue)
    println("Inside GPU FUSE")

end


function dofusion(queue)
    # create a dependecy graph
    # use graph.jl
    g = SimpleDiGraph() # empty grph
    task_to_id = IdDict{Any,Int}()
    for (spec, task) in collect(queue.running_tasks)
        
        println("Printing Spec: ", task)
        # task_id = task_to_id[task] = add_vertex(g)
        
        # for dep in spec.options.syncdeps
        #     if(dep ) 
        #         arg_id = task_to_id[arg]
        #         add_edge(g,task_id, arg_id)
        #     end
        # end
    end
end



# function dofusion(queue)
#     # create a dependecy graph
#     # use graph.jl
#     g = SimpleDiGraph() # empty grph
#     task_to_id = IdDict{Any,Int}()
#     for ref,spec in queue.running_tasks
#         println(spec)
#         task = WeakRef(ref)
#         if(task != nothing)
#             println("Processing Task: ", task)
#         else
#             println("Task was garbage collected.")
#         end
#     end
# end
