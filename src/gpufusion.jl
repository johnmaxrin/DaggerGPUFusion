using TaskLocalValues
using Graphs
export gpufuse


struct FusionTaskQueue <: AbstractTaskQueue
    running_tasks::Vector{}
    FusionTaskQueue() = new([])
end


function Dagger.enqueue!(queue::FusionTaskQueue, spec::Pair{Dagger.DTaskSpec, DTask})
     push!(queue.running_tasks, (spec))    
end

function gpufuse(f)
    queue = FusionTaskQueue()
    with_options(f; task_queue = queue) 
    dofusion(queue)

    # upper = get_options(:task_queue, EagerTaskQueue())
    # enqueue!(upper, queue.running_tasks)
    # println(queue)

end


function dofusion(queue)
    g = SimpleDiGraph() # empty grph
    task_to_id = IdDict{Int,Any}()
    for (spec, task) in collect(queue.running_tasks)
        

        add_vertex!(g)
        task_id = nv(g)
        task_to_id[task_id] = spec
    
    end

    println("Dict \n", task_to_id[1])
end



```
Updates

1. I created a dictionary with vertex IDs (task IDs) and their 
specifications, along with a graph, but I’m unsure how to 
determine dependencies using the tasks and specifications. 

```
